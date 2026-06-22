// Package proxy implements the OpenAI-compatible HTTP handler for
// /v1/chat/completions. It drives the hedge engine through a Sink that either
// writes a streaming SSE response or accumulates a single non-streaming JSON
// body.
//
// Header-commit timing is the key correctness property (see hedge.Sink): the
// handler writes response headers / 200 only when the engine commits a winner,
// so an all-backends-fail case returns a clean error JSON + status with nothing
// half-streamed.
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"hedge-llm/internal/hedge"
	"hedge-llm/internal/oapi"
)

// MetricsReporter receives per-request outcomes so the proxy can update metrics
// without importing the metrics package directly (keeps the dependency one-way).
type MetricsReporter interface {
	IncRequests()
	AddRedundantRequests(n int)
	ObserveWin(backend string)
	ObserveFirstToken(d time.Duration)
	AddLatencySaved(d time.Duration)
}

// LatencyObserver records a per-backend first-token latency sample for adaptive
// timing. Implemented by *adaptive.Estimator.
type LatencyObserver interface {
	Observe(backend string, firstToken time.Duration)
}

// Handler is the /v1/chat/completions HTTP handler.
type Handler struct {
	engine   *hedge.Engine
	metrics  MetricsReporter
	latency  LatencyObserver
	maxBytes int64
}

// Option configures a Handler.
type Option func(*Handler)

// WithMetrics installs a metrics reporter.
func WithMetrics(m MetricsReporter) Option { return func(h *Handler) { h.metrics = m } }

// WithLatencyObserver installs a per-backend first-token latency observer
// (adaptive timing).
func WithLatencyObserver(l LatencyObserver) Option { return func(h *Handler) { h.latency = l } }

// WithMaxRequestBytes caps the accepted request body size (default 1 MiB).
func WithMaxRequestBytes(n int64) Option {
	return func(h *Handler) {
		if n > 0 {
			h.maxBytes = n
		}
	}
}

// NewHandler builds a Handler for the given engine.
func NewHandler(engine *hedge.Engine, opts ...Option) *Handler {
	h := &Handler{engine: engine, maxBytes: 1 << 20}
	for _, o := range opts {
		o(h)
	}
	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body", "invalid_request_error", "")
		return
	}
	req, err := oapi.DecodeRequest(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body", "invalid_request_error", "")
		return
	}
	if len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "request must include at least one message", "invalid_request_error", "")
		return
	}

	if h.metrics != nil {
		h.metrics.IncRequests()
	}

	if req.Stream {
		h.serveStream(w, r, req)
		return
	}
	h.serveJSON(w, r, req)
}

// serveStream handles a streaming (SSE) request. It asserts http.Flusher at the
// start (else 500) and writes SSE only once the engine commits a winner.
func (h *Handler) serveStream(w http.ResponseWriter, r *http.Request, req *oapi.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported by server", "server_error", "")
		return
	}
	sink := &sseSink{w: w, flusher: flusher}
	outcome, err := h.engine.Run(r.Context(), req, sink)
	h.report(outcome, req)

	if !sink.committed {
		// Nothing was written yet: return a clean error response.
		h.writeUncommittedError(w, err)
		return
	}
	// Headers/200 already sent. Append the SSE terminator and flush. (If the
	// client disconnected mid-stream, these writes are best-effort.)
	sink.finish()
}

// serveJSON handles a non-streaming request, accumulating the winner's content
// into one JSON response body. Upstreams are always streamed internally; the
// accumulator joins the deltas.
func (h *Handler) serveJSON(w http.ResponseWriter, r *http.Request, req *oapi.Request) {
	sink := &jsonSink{}
	outcome, err := h.engine.Run(r.Context(), req, sink)
	h.report(outcome, req)

	if !sink.committed {
		h.writeUncommittedError(w, err)
		return
	}
	resp := oapi.Response{
		ID:      firstNonEmpty(sink.id, "chatcmpl-hedge"),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   firstNonEmpty(sink.model, req.Model),
		Choices: []oapi.Choice{{
			Index:        0,
			Message:      oapi.ResponseMessage{Role: "assistant", Content: sink.content.String()},
			FinishReason: firstNonEmpty(sink.finishReason, "stop"),
		}},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// writeUncommittedError maps an engine error to a clean error response. It is
// only called when no headers have been written yet.
func (h *Handler) writeUncommittedError(w http.ResponseWriter, err error) {
	switch {
	case err == nil, errors.Is(err, hedge.ErrAllBackendsFailed):
		writeError(w, http.StatusBadGateway,
			"all backends failed to produce a response", "upstream_error", "all_backends_failed")
	case errors.Is(err, hedge.ErrNoBackends):
		writeError(w, http.StatusServiceUnavailable,
			"no backends configured", "server_error", "no_backends")
	case isContextCanceled(err):
		// Client went away before any winner; nothing to send.
		writeError(w, 499, "client closed request", "client_error", "client_closed")
	default:
		writeError(w, http.StatusBadGateway, err.Error(), "upstream_error", "")
	}
}

// report pushes the outcome to metrics and the latency observer.
func (h *Handler) report(o hedge.Outcome, _ *oapi.Request) {
	if h.metrics != nil {
		h.metrics.AddRedundantRequests(o.RedundantStarts())
		if o.Winner != "" {
			h.metrics.ObserveWin(o.Winner)
			h.metrics.ObserveFirstToken(o.FirstTokenLatency)
			h.metrics.AddLatencySaved(o.LatencySaved())
		}
	}
	if h.latency != nil && o.Winner != "" && o.FirstTokenLatency > 0 {
		h.latency.Observe(o.Winner, o.FirstTokenLatency)
	}
}

// --- sinks ------------------------------------------------------------------

// sseSink writes the winner's chunks as OpenAI-style SSE, flushing each.
type sseSink struct {
	w         http.ResponseWriter
	flusher   http.Flusher
	committed bool
	writeErr  bool
}

// Commit writes SSE headers/200 and emits the first chunk.
func (s *sseSink) Commit(_ string, first oapi.Chunk) error {
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	s.w.WriteHeader(http.StatusOK)
	s.committed = true
	return s.Chunk(first)
}

// Chunk relays one chunk as a `data: {json}\n\n` SSE event, flushing it.
func (s *sseSink) Chunk(c oapi.Chunk) error {
	if s.writeErr {
		return io.ErrClosedPipe
	}
	payload := c.Raw
	if len(payload) == 0 {
		var err error
		payload, err = json.Marshal(c)
		if err != nil {
			return err
		}
	}
	if _, err := s.w.Write([]byte("data: ")); err != nil {
		s.writeErr = true
		return err
	}
	if _, err := s.w.Write(payload); err != nil {
		s.writeErr = true
		return err
	}
	if _, err := s.w.Write([]byte("\n\n")); err != nil {
		s.writeErr = true
		return err
	}
	s.flusher.Flush()
	return nil
}

// finish writes the terminal `data: [DONE]` event (best-effort).
func (s *sseSink) finish() {
	if s.writeErr {
		return
	}
	_, _ = io.WriteString(s.w, "data: [DONE]\n\n")
	s.flusher.Flush()
}

// jsonSink accumulates the winner's content for a non-streaming response.
type jsonSink struct {
	committed    bool
	content      strings.Builder
	id           string
	model        string
	finishReason string
}

// Commit records the first chunk's metadata and its content.
func (s *jsonSink) Commit(_ string, first oapi.Chunk) error {
	s.committed = true
	s.id = first.ID
	s.model = first.Model
	return s.Chunk(first)
}

// Chunk appends a chunk's content delta and tracks the finish reason.
func (s *jsonSink) Chunk(c oapi.Chunk) error {
	for _, ch := range c.Choices {
		if ch.Delta.Content != "" {
			s.content.WriteString(ch.Delta.Content)
		}
		if ch.FinishReason != nil && *ch.FinishReason != "" {
			s.finishReason = *ch.FinishReason
		}
	}
	return nil
}

// --- helpers ----------------------------------------------------------------

func writeError(w http.ResponseWriter, status int, message, typ, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(oapi.NewError(message, typ, code))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func isContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
