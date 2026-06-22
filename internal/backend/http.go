package backend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"hedge-llm/internal/oapi"
)

// HTTPBackend is a generic [Backend] for an OpenAI-compatible streaming upstream
// (OpenAI, Azure OpenAI, Ollama's /v1, vLLM, etc.). It POSTs the chat-completion
// request with stream=true and parses the Server-Sent-Events response into
// oapi.Chunk values.
//
// Non-OpenAI provider shapes (Anthropic, Gemini) are a documented extension
// point: a future adapter would translate their event streams into oapi.Chunk
// here. v1 targets OpenAI-compatible upstreams.
type HTTPBackend struct {
	// BackendName identifies the backend in metrics/logs.
	BackendName string
	// BaseURL is the upstream root, e.g. "https://api.openai.com/v1". The
	// "/chat/completions" path is appended automatically.
	BaseURL string
	// APIKey is sent as a Bearer token when non-empty.
	APIKey string
	// Model overrides the request model when forwarding upstream.
	Model string
	// Cost is the relative speculative cost reported by CostPerRequest.
	Cost float64
	// Client is the HTTP client used for upstream requests. Required.
	Client *http.Client
}

// NewHTTPBackend constructs an HTTPBackend with a sane default client if none
// is supplied. The default client has no overall timeout (streaming responses
// are long-lived); per-request lifetime is governed by the caller's context.
func NewHTTPBackend(name, baseURL, apiKey, model string, cost float64, client *http.Client) *HTTPBackend {
	if client == nil {
		client = &http.Client{
			// No Timeout: streaming requests must stay open. Cancellation is
			// driven by the request context from the hedge engine.
			Transport: &http.Transport{
				MaxIdleConns:        100,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		}
	}
	return &HTTPBackend{
		BackendName: name,
		BaseURL:     strings.TrimRight(baseURL, "/"),
		APIKey:      apiKey,
		Model:       model,
		Cost:        cost,
		Client:      client,
	}
}

// Name implements Backend.
func (b *HTTPBackend) Name() string { return b.BackendName }

// CostPerRequest implements Backend.
func (b *HTTPBackend) CostPerRequest() float64 { return b.Cost }

// Stream implements Backend for an OpenAI-compatible upstream. It obeys the
// cancellation-safe send contract: the producer goroutine selects on
// ctx.Done() for every send, closes the channel on return, and closes the
// upstream response body via defer so no connection or goroutine leaks when the
// engine cancels a loser.
func (b *HTTPBackend) Stream(ctx context.Context, req *oapi.Request) (<-chan oapi.Chunk, error) {
	body, err := req.EncodeUpstream(b.Model)
	if err != nil {
		return nil, fmt.Errorf("hedge-llm: encode upstream request: %w", err)
	}
	url := b.BaseURL + "/chat/completions"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("hedge-llm: build upstream request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if b.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+b.APIKey)
	}

	resp, err := b.Client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("hedge-llm: upstream %s: %w", b.BackendName, err)
	}
	if resp.StatusCode != http.StatusOK {
		// Drain a bounded amount of the body for the error message, then close.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("hedge-llm: upstream %s returned %d: %s",
			b.BackendName, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	ch := make(chan oapi.Chunk, 1)
	go func() {
		defer close(ch)
		defer func() { _ = resp.Body.Close() }()
		b.parseSSE(ctx, resp.Body, ch)
	}()
	return ch, nil
}

// parseSSE reads an OpenAI-style SSE stream and forwards each data chunk on ch,
// selecting on ctx.Done() for every send. It returns when the stream ends, on
// "[DONE]", on a read error, or when the context is cancelled.
func (b *HTTPBackend) parseSSE(ctx context.Context, body io.Reader, ch chan<- oapi.Chunk) {
	scanner := bufio.NewScanner(body)
	// Allow large SSE lines (some chunks are sizeable).
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		// Cheap cancellation check between lines; the send below is the
		// authoritative cancellation point.
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, ":") {
			// Blank separator line or SSE comment/heartbeat.
			continue
		}
		const dataPrefix = "data:"
		if !strings.HasPrefix(line, dataPrefix) {
			// Ignore non-data fields (event:, id:, retry:).
			continue
		}
		payload := strings.TrimSpace(line[len(dataPrefix):])
		if payload == "[DONE]" {
			return
		}
		var chunk oapi.Chunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			// Skip malformed chunks rather than aborting the stream.
			continue
		}
		chunk.Raw = []byte(payload)
		select {
		case ch <- chunk:
		case <-ctx.Done():
			return
		}
	}
	// scanner.Err() is intentionally not surfaced as a separate event: the
	// engine treats a channel close with no usable token as a loss regardless
	// of the underlying cause (EOF, read error, or cancellation).
}

// mustMarshalChunk serialises a chunk to its JSON payload. It is used to
// populate Chunk.Raw for synthetic chunks (FakeBackend) so relayed bytes look
// like a genuine upstream chunk. Marshalling these simple structs cannot fail.
func mustMarshalChunk(c oapi.Chunk) []byte {
	out, err := json.Marshal(c)
	if err != nil {
		// Unreachable for the fixed chunk shape; fall back to an empty object.
		return []byte("{}")
	}
	return out
}
