package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"hedge-llm/internal/backend"
	"hedge-llm/internal/clock"
	"hedge-llm/internal/hedge"
	"hedge-llm/internal/metrics"
	"hedge-llm/internal/oapi"
	"hedge-llm/internal/policy"
)

// driveClock advances a FakeClock until stopped; returns a stop func.
func driveClock(clk *clock.FakeClock, step time.Duration) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				clk.Advance(step)
			}
		}
	}()
	return func() { close(stop); <-done }
}

func newTestHandler(t *testing.T, backends []backend.Backend, pol policy.HedgePolicy, clk clock.Clock, reg *metrics.Registry) *Handler {
	t.Helper()
	e := hedge.NewEngine(backends, pol, clk)
	opts := []Option{}
	if reg != nil {
		opts = append(opts, WithMetrics(reg))
	}
	return NewHandler(e, opts...)
}

func postBody(stream bool) string {
	s := "false"
	if stream {
		s = "true"
	}
	return `{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":` + s + `}`
}

func TestServeStreamSSEShape(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		InterTokenDelay: 3 * time.Millisecond,
		Tokens:          []string{"Hello", " world"}, EmitFinish: true,
	}
	h := newTestHandler(t, []backend.Backend{primary}, policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}, clk, nil)

	stop := driveClock(clk, 2*time.Millisecond)
	defer stop()

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(true)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type=%q", ct)
	}

	var dataLines []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
		}
	}
	if len(dataLines) == 0 {
		t.Fatal("no SSE data lines")
	}
	// Last line must be the terminal [DONE].
	if dataLines[len(dataLines)-1] != "[DONE]" {
		t.Errorf("last data line=%q want [DONE]", dataLines[len(dataLines)-1])
	}
	// Reassemble content from the chunk JSONs (excluding [DONE]).
	var content strings.Builder
	for _, d := range dataLines[:len(dataLines)-1] {
		var c oapi.Chunk
		if err := json.Unmarshal([]byte(d), &c); err != nil {
			t.Fatalf("bad chunk JSON %q: %v", d, err)
		}
		content.WriteString(c.UsableContent())
	}
	if content.String() != "Hello world" {
		t.Errorf("streamed content=%q want %q", content.String(), "Hello world")
	}
}

func TestServeNonStreamJSON(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		InterTokenDelay: 3 * time.Millisecond,
		Tokens:          []string{"foo", "bar"}, EmitFinish: true,
	}
	h := newTestHandler(t, []backend.Backend{primary}, policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}, clk, nil)

	stop := driveClock(clk, 2*time.Millisecond)
	defer stop()

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(false)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type=%q", ct)
	}
	var out oapi.Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "foobar" {
		t.Errorf("content=%q want foobar (choices=%+v)", out.Choices[0].Message.Content, out.Choices)
	}
	if out.Choices[0].Message.Role != "assistant" {
		t.Errorf("role=%q", out.Choices[0].Message.Role)
	}
	if out.Object != "chat.completion" {
		t.Errorf("object=%q", out.Object)
	}
}

func TestAllBackendsFailReturnsErrorJSON(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Backends close with no usable token → all lose.
	mk := func(n string) *backend.FakeBackend {
		return &backend.FakeBackend{BackendName: n, Clock: clk, FirstTokenDelay: 2 * time.Millisecond, EmitFinish: true}
	}
	h := newTestHandler(t, []backend.Backend{mk("a"), mk("b")}, policy.HedgePolicy{FireAfter: 2 * time.Millisecond, MaxInFlight: 2}, clk, nil)

	stop := driveClock(clk, time.Millisecond)
	defer stop()

	srv := httptest.NewServer(h)
	defer srv.Close()

	for _, stream := range []bool{false, true} {
		resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(stream)))
		if err != nil {
			t.Fatal(err)
		}
		// Nothing committed → clean error status (502) + error JSON.
		if resp.StatusCode != http.StatusBadGateway {
			t.Errorf("stream=%v status=%d want 502", stream, resp.StatusCode)
		}
		var errBody oapi.ErrorBody
		if err := json.NewDecoder(resp.Body).Decode(&errBody); err != nil {
			t.Errorf("stream=%v: decode error body: %v", stream, err)
		}
		_ = resp.Body.Close()
		if errBody.Error.Message == "" {
			t.Errorf("stream=%v: empty error message", stream)
		}
	}
}

func TestInvalidRequests(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{BackendName: "p", Clock: clk, Tokens: []string{"x"}}
	h := newTestHandler(t, []backend.Backend{primary}, policy.DefaultPolicy(), clk, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Bad JSON.
	resp, _ := http.Post(srv.URL, "application/json", strings.NewReader(`{bad`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad json status=%d want 400", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Empty messages.
	resp, _ = http.Post(srv.URL, "application/json", strings.NewReader(`{"model":"m","messages":[]}`))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty messages status=%d want 400", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Wrong method.
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET status=%d want 405", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

// The handler must surface a 500 if the ResponseWriter is not a Flusher for a
// streaming request. We call ServeHTTP directly with a non-flushing recorder.
func TestStreamRequiresFlusher(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{BackendName: "p", Clock: clk, Tokens: []string{"x"}}
	h := newTestHandler(t, []backend.Backend{primary}, policy.DefaultPolicy(), clk, nil)

	rw := &nonFlusherRecorder{header: http.Header{}}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(postBody(true)))
	h.ServeHTTP(rw, req)
	if rw.status != http.StatusInternalServerError {
		t.Errorf("status=%d want 500 when Flusher unsupported", rw.status)
	}
}

func TestClientDisconnectCancels(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Backend never produces a usable token in test time; client disconnects.
	primary := &backend.FakeBackend{
		BackendName: "p", Clock: clk,
		FirstTokenDelay: time.Hour, Tokens: []string{"x"},
	}
	h := newTestHandler(t, []backend.Backend{primary}, policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}, clk, nil)

	stop := driveClock(clk, time.Millisecond)
	defer stop()

	srv := httptest.NewServer(h)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader(postBody(true)))

	errCh := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		errCh <- err
	}()
	// Cancel the client request mid-flight.
	time.Sleep(40 * time.Millisecond)
	cancel()

	select {
	case <-errCh:
		// The HTTP client returns (cancelled). The server-side handler must also
		// have returned; give it a moment, then assert no in-flight remains is
		// covered by the engine tests. Here we just ensure no hang.
	case <-time.After(3 * time.Second):
		t.Fatal("client request did not return after cancel")
	}
}

func TestMetricsRecordedOnRequest(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"hi"}, EmitFinish: true,
	}
	reg := metrics.NewRegistry(nil)
	h := newTestHandler(t, []backend.Backend{primary}, policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}, clk, reg)

	stop := driveClock(clk, 2*time.Millisecond)
	defer stop()

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(false)))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	var b strings.Builder
	_, _ = reg.WriteTo(&b)
	out := b.String()
	if !strings.Contains(out, "hedge_requests_total 1") {
		t.Errorf("requests_total not incremented:\n%s", out)
	}
	if !strings.Contains(out, `hedge_backend_wins_total{backend="primary"} 1`) {
		t.Errorf("backend win not recorded:\n%s", out)
	}
}

// recordingObserver captures latency observations for the adaptive wiring test.
type recordingObserver struct {
	mu    sync.Mutex
	calls []struct {
		backend string
		d       time.Duration
	}
}

func (o *recordingObserver) Observe(backend string, firstToken time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, struct {
		backend string
		d       time.Duration
	}{backend, firstToken})
}

func TestLatencyObserverWired(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"hi"}, EmitFinish: true,
	}
	e := hedge.NewEngine([]backend.Backend{primary}, policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}, clk)
	obs := &recordingObserver{}
	h := NewHandler(e, WithLatencyObserver(obs), WithMaxRequestBytes(1<<16))

	stop := driveClock(clk, 2*time.Millisecond)
	defer stop()

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(false)))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.calls) != 1 || obs.calls[0].backend != "primary" {
		t.Fatalf("observer calls=%+v want one for primary", obs.calls)
	}
	if obs.calls[0].d <= 0 {
		t.Errorf("observed first-token latency=%v want >0", obs.calls[0].d)
	}
}

func TestNoBackendsReturnsServiceUnavailable(t *testing.T) {
	e := hedge.NewEngine(nil, policy.DefaultPolicy(), clock.NewFakeClock(time.Time{}))
	h := NewHandler(e)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(false)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status=%d want 503", resp.StatusCode)
	}
	var eb oapi.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&eb); err != nil {
		t.Fatal(err)
	}
	if eb.Error.Code != "no_backends" {
		t.Errorf("error code=%q want no_backends", eb.Error.Code)
	}
}

func TestAuthRejectsMissingHeader(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{BackendName: "p", Clock: clk, Tokens: []string{"x"}}
	e := hedge.NewEngine([]backend.Backend{primary}, policy.DefaultPolicy(), clk)
	h := NewHandler(e, WithAPIKey("sk-secret"))
	srv := httptest.NewServer(h)
	defer srv.Close()

	// No Authorization header at all.
	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(false)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status=%d want 401", resp.StatusCode)
	}
	var eb oapi.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&eb); err != nil {
		t.Fatal(err)
	}
	if eb.Error.Code != "invalid_api_key" {
		t.Errorf("error code=%q want invalid_api_key", eb.Error.Code)
	}
	if eb.Error.Type != "invalid_request_error" {
		t.Errorf("error type=%q want invalid_request_error", eb.Error.Type)
	}
}

func TestAuthRejectsWrongKey(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{BackendName: "p", Clock: clk, Tokens: []string{"x"}}
	e := hedge.NewEngine([]backend.Backend{primary}, policy.DefaultPolicy(), clk)
	h := NewHandler(e, WithAPIKey("sk-secret"))
	srv := httptest.NewServer(h)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(postBody(false)))
	req.Header.Set("Authorization", "Bearer wrong-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status=%d want 401", resp.StatusCode)
	}
}

func TestAuthRejectsMalformedHeader(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{BackendName: "p", Clock: clk, Tokens: []string{"x"}}
	e := hedge.NewEngine([]backend.Backend{primary}, policy.DefaultPolicy(), clk)
	h := NewHandler(e, WithAPIKey("sk-secret"))
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Right key, wrong scheme.
	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(postBody(false)))
	req.Header.Set("Authorization", "sk-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status=%d want 401", resp.StatusCode)
	}
}

func TestAuthAllowsCorrectKey(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "p", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"hi"}, EmitFinish: true,
	}
	e := hedge.NewEngine([]backend.Backend{primary}, policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}, clk)
	h := NewHandler(e, WithAPIKey("sk-secret"))

	stop := driveClock(clk, 2*time.Millisecond)
	defer stop()

	srv := httptest.NewServer(h)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL, strings.NewReader(postBody(false)))
	req.Header.Set("Authorization", "Bearer sk-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status=%d want 200", resp.StatusCode)
	}
}

func TestAuthDisabledByDefaultAllowsNoHeader(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "p", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"hi"}, EmitFinish: true,
	}
	e := hedge.NewEngine([]backend.Backend{primary}, policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}, clk)
	h := NewHandler(e) // no WithAPIKey: auth stays off, matching pre-issue-9 behavior.

	stop := driveClock(clk, 2*time.Millisecond)
	defer stop()

	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(false)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status=%d want 200 (auth disabled by default)", resp.StatusCode)
	}
}

func TestMaxRequestBytesTruncatesToInvalid(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{BackendName: "p", Clock: clk, Tokens: []string{"x"}}
	e := hedge.NewEngine([]backend.Backend{primary}, policy.DefaultPolicy(), clk)
	// Cap at 10 bytes so any real JSON body is truncated → parse error → 400.
	h := NewHandler(e, WithMaxRequestBytes(10))
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", strings.NewReader(postBody(false)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status=%d want 400 (truncated body)", resp.StatusCode)
	}
}

// nonFlusherRecorder is a ResponseWriter that does NOT implement http.Flusher.
type nonFlusherRecorder struct {
	header http.Header
	status int
	body   strings.Builder
}

func (r *nonFlusherRecorder) Header() http.Header { return r.header }
func (r *nonFlusherRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(b)
}
func (r *nonFlusherRecorder) WriteHeader(s int) { r.status = s }
