package backend

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hedge-llm/internal/oapi"
)

// sseServer returns an httptest server that streams the given SSE data lines
// then "[DONE]".
func sseServer(t *testing.T, dataLines []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server lacks Flusher")
			return
		}
		w.WriteHeader(http.StatusOK)
		for _, d := range dataLines {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", d)
			fl.Flush()
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
}

func req() *oapi.Request {
	r, _ := oapi.DecodeRequest([]byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	return r
}

func TestHTTPBackendParsesSSE(t *testing.T) {
	srv := sseServer(t, []string{
		`{"id":"1","model":"up","choices":[{"index":0,"delta":{"role":"assistant"}}]}`,
		`{"id":"1","model":"up","choices":[{"index":0,"delta":{"content":"Hello"}}]}`,
		`{"id":"1","model":"up","choices":[{"index":0,"delta":{"content":" world"}}]}`,
	})
	defer srv.Close()

	b := NewHTTPBackend("up", srv.URL, "", "up-model", 1, srv.Client())
	ch, err := b.Stream(context.Background(), req())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	got := drain(t, ch)
	if len(got) != 3 {
		t.Fatalf("got %d chunks want 3", len(got))
	}
	if got[1].UsableContent() != "Hello" || got[2].UsableContent() != " world" {
		t.Errorf("contents wrong: %q %q", got[1].UsableContent(), got[2].UsableContent())
	}
	if len(got[1].Raw) == 0 {
		t.Error("Raw should be populated from upstream payload")
	}
}

func TestHTTPBackendNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprint(w, `{"error":"rate limited"}`)
	}))
	defer srv.Close()
	b := NewHTTPBackend("up", srv.URL, "key", "m", 1, srv.Client())
	_, err := b.Stream(context.Background(), req())
	if err == nil {
		t.Fatal("expected error for non-200 status")
	}
}

func TestHTTPBackendSendsAuthAndStream(t *testing.T) {
	var gotAuth, gotAccept string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotAccept = r.Header.Get("Accept")
		sc := bufio.NewScanner(r.Body)
		if sc.Scan() {
			gotBody = sc.Bytes()
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		fl.Flush()
	}))
	defer srv.Close()

	b := NewHTTPBackend("up", srv.URL, "sk-abc", "forced-model", 1, srv.Client())
	ch, err := b.Stream(context.Background(), req())
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ch)

	if gotAuth != "Bearer sk-abc" {
		t.Errorf("Authorization=%q", gotAuth)
	}
	if gotAccept != "text/event-stream" {
		t.Errorf("Accept=%q", gotAccept)
	}
	if !containsAll(string(gotBody), `"model":"forced-model"`, `"stream":true`) {
		t.Errorf("upstream body wrong: %s", gotBody)
	}
}

func TestHTTPBackendCancelClosesChannel(t *testing.T) {
	// Server streams slowly and indefinitely; cancellation must close the chan.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 1000; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			_, _ = fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n")
			fl.Flush()
			time.Sleep(5 * time.Millisecond)
		}
	}))
	defer srv.Close()

	b := NewHTTPBackend("up", srv.URL, "", "m", 1, srv.Client())
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := b.Stream(ctx, req())
	if err != nil {
		t.Fatal(err)
	}
	// Read one chunk, then cancel.
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("no chunk received")
	}
	cancel()
	// The channel must close promptly after cancellation.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed: success
			}
		case <-deadline:
			t.Fatal("channel did not close after cancel")
		}
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
