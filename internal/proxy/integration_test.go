package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"hedge-llm/internal/backend"
	"hedge-llm/internal/clock"
	"hedge-llm/internal/hedge"
	"hedge-llm/internal/metrics"
	"hedge-llm/internal/oapi"
	"hedge-llm/internal/policy"
)

// TestEndToEndHTTPBackend wires a real httptest upstream through an HTTPBackend,
// the hedge engine, and the proxy handler — the same path cmd/hedge-llm builds —
// using the real clock. It validates that the full stack produces a correct
// non-streaming response and updates metrics, with no fakes in the request path.
func TestEndToEndHTTPBackend(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `data: {"id":"up1","object":"chat.completion.chunk","model":"up","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"}}]}`+"\n\n")
		fl.Flush()
		_, _ = fmt.Fprint(w, `data: {"id":"up1","object":"chat.completion.chunk","model":"up","choices":[{"index":0,"delta":{"content":" world"}}]}`+"\n\n")
		fl.Flush()
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	defer upstream.Close()

	be := backend.NewHTTPBackend("upstream", upstream.URL, "", "up-model", 1, upstream.Client())
	reg := metrics.NewRegistry(nil)
	engine := hedge.NewEngine([]backend.Backend{be}, policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}, clock.RealClock{})
	reg.SetInFlightFunc(engine.InFlight)
	h := NewHandler(engine, WithMetrics(reg))

	srv := httptest.NewServer(h)
	defer srv.Close()

	// Non-streaming request.
	resp, err := http.Post(srv.URL, "application/json",
		strings.NewReader(`{"model":"client","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var out oapi.Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "Hello world" {
		t.Fatalf("content=%q want %q", out.Choices[0].Message.Content, "Hello world")
	}

	// Metrics reflect the request and the win.
	var b strings.Builder
	_, _ = reg.WriteTo(&b)
	text := b.String()
	if !strings.Contains(text, "hedge_requests_total 1") {
		t.Errorf("requests_total missing:\n%s", text)
	}
	if !strings.Contains(text, `hedge_backend_wins_total{backend="upstream"} 1`) {
		t.Errorf("win not recorded:\n%s", text)
	}
	// Inflight gauge should be back to 0 after the request completed.
	if !strings.Contains(text, "hedge_inflight 0") {
		t.Errorf("inflight not 0 after completion:\n%s", text)
	}
}
