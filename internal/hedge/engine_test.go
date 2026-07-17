package hedge

import (
	"bufio"
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"hedge-llm/internal/adaptive"
	"hedge-llm/internal/backend"
	"hedge-llm/internal/clock"
	"hedge-llm/internal/metrics"
	"hedge-llm/internal/oapi"
	"hedge-llm/internal/policy"
)

// ---- test sink -------------------------------------------------------------

// captureSink records commit + chunks. Its writes never fail unless failOn is
// set, letting tests simulate a client write error.
type captureSink struct {
	mu        sync.Mutex
	committed bool
	winner    string
	contents  []string
	failOn    int // return error on the Nth Chunk call (1-based); 0 = never
	calls     int
}

func (s *captureSink) Commit(backend string, first oapi.Chunk) error {
	s.mu.Lock()
	s.committed = true
	s.winner = backend
	s.mu.Unlock()
	return s.Chunk(first)
}

func (s *captureSink) Chunk(c oapi.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failOn > 0 && s.calls == s.failOn {
		return errors.New("simulated client write failure")
	}
	if u := c.UsableContent(); u != "" {
		s.contents = append(s.contents, u)
	}
	return nil
}

func (s *captureSink) snapshot() (committed bool, winner string, contents []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed, s.winner, append([]string(nil), s.contents...)
}

// ---- clock driver ----------------------------------------------------------

// clockDriver advances a FakeClock in small steps on a background goroutine so
// the engine's and backends' clock-driven timers progress deterministically.
// The logical timing is entirely clock-driven; the real sleeps only yield the
// scheduler so dependent goroutines can run between advances.
type clockDriver struct {
	clk  *clock.FakeClock
	stop chan struct{}
	done chan struct{}
}

func startDriver(clk *clock.FakeClock, step time.Duration) *clockDriver {
	d := &clockDriver{clk: clk, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(d.done)
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-d.stop:
				return
			case <-t.C:
				clk.Advance(step)
			}
		}
	}()
	return d
}

func (d *clockDriver) Stop() {
	close(d.stop)
	<-d.done
}

func testReq() *oapi.Request {
	return &oapi.Request{Model: "m", Messages: []oapi.Message{{Role: "user", Content: "hi"}}}
}

// runEngine runs the engine to completion in a goroutine with a real-time
// safety timeout, returning the outcome and error.
func runEngine(t *testing.T, e *Engine, ctx context.Context, sink Sink) (Outcome, error) {
	t.Helper()
	type res struct {
		o   Outcome
		err error
	}
	ch := make(chan res, 1)
	go func() {
		o, err := e.Run(ctx, testReq(), sink)
		ch <- res{o, err}
	}()
	select {
	case r := <-ch:
		return r.o, r.err
	case <-time.After(10 * time.Second):
		t.Fatal("engine.Run did not return (possible deadlock)")
		return Outcome{}, nil
	}
}

// ---- tests -----------------------------------------------------------------

// Primary wins before fire_after → no backup started.
func TestPrimaryWinsNoBackup(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 10 * time.Millisecond,
		Tokens:          []string{"P1", "P2"}, EmitFinish: true,
	}
	backup := &backend.FakeBackend{
		BackendName: "backup", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"B1"}, EmitFinish: true,
	}
	pol := policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 2} // fire_after >> primary delay
	e := NewEngine([]backend.Backend{primary, backup}, pol, clk)

	d := startDriver(clk, 2*time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if o.Winner != "primary" {
		t.Errorf("winner=%q want primary", o.Winner)
	}
	if o.Started != 1 {
		t.Errorf("started=%d want 1 (no backup)", o.Started)
	}
	_, winner, contents := sink.snapshot()
	if winner != "primary" {
		t.Errorf("sink winner=%q", winner)
	}
	if len(contents) != 2 || contents[0] != "P1" {
		t.Errorf("contents=%v want [P1 P2]", contents)
	}
}

// Primary slow → backup fires at fire_after and wins → primary cancelled.
func TestPrimarySlowBackupWins(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: time.Hour, // never produces in test time
		Tokens:          []string{"P"},
	}
	backup := &backend.FakeBackend{
		BackendName: "backup", Clock: clk,
		FirstTokenDelay: 10 * time.Millisecond,
		Tokens:          []string{"B1", "B2"}, EmitFinish: true,
	}
	pol := policy.HedgePolicy{FireAfter: 20 * time.Millisecond, MaxInFlight: 2}
	e := NewEngine([]backend.Backend{primary, backup}, pol, clk)

	d := startDriver(clk, 2*time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if o.Winner != "backup" {
		t.Errorf("winner=%q want backup", o.Winner)
	}
	if o.Started != 2 {
		t.Errorf("started=%d want 2", o.Started)
	}
	_, _, contents := sink.snapshot()
	if len(contents) != 2 || contents[0] != "B1" {
		t.Errorf("contents=%v want [B1 B2]", contents)
	}
}

// A heartbeat/empty/finish-only delta does not win; the first NON-EMPTY content
// delta defines the winner.
func TestUsableTokenDefinesWinner(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Primary emits empty deltas (heartbeats) then a real token slightly later.
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		// Empty strings model heartbeats; only "REAL" is usable.
		Tokens:          []string{"", "", "REAL"},
		InterTokenDelay: 5 * time.Millisecond,
		EmitFinish:      true,
	}
	pol := policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}
	e := NewEngine([]backend.Backend{primary}, pol, clk)

	d := startDriver(clk, 1*time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if o.Winner != "primary" {
		t.Errorf("winner=%q", o.Winner)
	}
	_, _, contents := sink.snapshot()
	if len(contents) != 1 || contents[0] != "REAL" {
		t.Errorf("contents=%v want [REAL] (heartbeats filtered)", contents)
	}
}

// A backend that closes with NO usable token is a LOSS (not a hang); the engine
// proceeds to the next backend.
func TestNoUsableTokenIsLossNotHang(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Primary emits only a finish chunk (no usable content) and closes → loss.
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          nil, EmitFinish: true,
	}
	backup := &backend.FakeBackend{
		BackendName: "backup", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"B"}, EmitFinish: true,
	}
	// fire_after large so the backup only starts because the primary LOST
	// (proving loss-driven progression, not timer-driven).
	pol := policy.HedgePolicy{FireAfter: time.Hour, MaxInFlight: 2}
	e := NewEngine([]backend.Backend{primary, backup}, pol, clk)

	d := startDriver(clk, 1*time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if o.Winner != "backup" {
		t.Errorf("winner=%q want backup (after primary loss)", o.Winner)
	}
	if o.Started != 2 {
		t.Errorf("started=%d want 2", o.Started)
	}
}

// All backends lose → engine returns ErrAllBackendsFailed and the sink is never
// committed (headers not written).
func TestAllBackendsLoseError(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	mk := func(name string) *backend.FakeBackend {
		return &backend.FakeBackend{
			BackendName: name, Clock: clk,
			FirstTokenDelay: 5 * time.Millisecond,
			Tokens:          nil, EmitFinish: true, // no usable token
		}
	}
	pol := policy.HedgePolicy{FireAfter: 5 * time.Millisecond, MaxInFlight: 3}
	e := NewEngine([]backend.Backend{mk("a"), mk("b"), mk("c")}, pol, clk)

	d := startDriver(clk, 1*time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if !errors.Is(err, ErrAllBackendsFailed) {
		t.Fatalf("err=%v want ErrAllBackendsFailed", err)
	}
	if o.Winner != "" {
		t.Errorf("winner=%q want empty", o.Winner)
	}
	if committed, _, _ := sink.snapshot(); committed {
		t.Error("sink must NOT be committed when all backends fail")
	}
}

// All backends error synchronously on Stream → engine returns an error, no leak.
func TestAllBackendsStartErrorError(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	mk := func(name string) *backend.FakeBackend {
		return &backend.FakeBackend{
			BackendName: name, Clock: clk,
			StartErr: errors.New("connect failed"),
		}
	}
	pol := policy.HedgePolicy{FireAfter: 5 * time.Millisecond, MaxInFlight: 2}
	e := NewEngine([]backend.Backend{mk("a"), mk("b")}, pol, clk)

	d := startDriver(clk, 1*time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	_, err := runEngine(t, e, context.Background(), sink)
	if !errors.Is(err, ErrAllBackendsFailed) {
		t.Fatalf("err=%v want ErrAllBackendsFailed", err)
	}
}

// max_in_flight caps the number of started backends even when many would fire.
func TestMaxInFlightCap(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	mk := func(name string) *backend.FakeBackend {
		return &backend.FakeBackend{
			BackendName: name, Clock: clk,
			FirstTokenDelay: time.Hour, // none ever produce → only the cap limits starts
			Tokens:          []string{"x"},
		}
	}
	// 4 backends available, fire_after tiny, but cap = 2.
	pol := policy.HedgePolicy{FireAfter: 5 * time.Millisecond, MaxInFlight: 2}
	e := NewEngine([]backend.Backend{mk("a"), mk("b"), mk("c"), mk("d")}, pol, clk)

	d := startDriver(clk, 2*time.Millisecond)

	// Run, let it spin firing timers, then cancel via a deadline so it returns.
	ctx, cancel := context.WithCancel(context.Background())
	type res struct {
		o   Outcome
		err error
	}
	ch := make(chan res, 1)
	go func() {
		o, err := e.Run(ctx, testReq(), &captureSink{})
		ch <- res{o, err}
	}()

	// Give the engine real time to fire its (capped) backups, then cancel.
	time.Sleep(150 * time.Millisecond)
	if got := e.InFlight(); got > 2 {
		t.Errorf("InFlight=%d exceeded cap 2", got)
	}
	cancel()
	d.Stop()

	r := <-ch
	if r.o.Started > 2 {
		t.Errorf("started=%d exceeded cap 2", r.o.Started)
	}
}

// cost_ceiling blocks an extra backend even with in-flight headroom.
func TestCostCeilingBlocks(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	mk := func(name string, cost float64) *backend.FakeBackend {
		return &backend.FakeBackend{
			BackendName: name, Clock: clk, Cost: cost,
			FirstTokenDelay: time.Hour,
			Tokens:          []string{"x"},
		}
	}
	// Each costs 1.0; ceiling 2.0 → at most 2 starts (1.0 + 1.0), the 3rd
	// (→3.0) is blocked even though MaxInFlight=10 leaves room.
	pol := policy.HedgePolicy{FireAfter: 3 * time.Millisecond, MaxInFlight: 10, CostCeiling: 2.0}
	e := NewEngine([]backend.Backend{mk("a", 1), mk("b", 1), mk("c", 1)}, pol, clk)

	d := startDriver(clk, 2*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan int, 1)
	go func() {
		o, _ := e.Run(ctx, testReq(), &captureSink{})
		ch <- o.Started
	}()
	time.Sleep(150 * time.Millisecond)
	if got := e.InFlight(); got > 2 {
		t.Errorf("InFlight=%d exceeded cost ceiling (max 2 starts)", got)
	}
	cancel()
	d.Stop()
	if started := <-ch; started > 2 {
		t.Errorf("started=%d exceeded cost ceiling", started)
	}
}

// The check-and-increment is atomic under concurrent Runs sharing one engine:
// the engine-wide InFlight never exceeds MaxInFlight even under load.
func TestConcurrentStartsRespectCapAtomically(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	mk := func(name string) *backend.FakeBackend {
		return &backend.FakeBackend{
			BackendName: name, Clock: clk,
			FirstTokenDelay: time.Hour, Tokens: []string{"x"},
		}
	}
	// Per-run cap of 3; many concurrent runs. We assert the GLOBAL in-flight
	// never goes negative or wildly high, and the check-and-increment under the
	// single mutex is race-free (the -race detector is the real assertion).
	pol := policy.HedgePolicy{FireAfter: time.Millisecond, MaxInFlight: 3}
	e := NewEngine([]backend.Backend{mk("a"), mk("b"), mk("c"), mk("d"), mk("e")}, pol, clk)

	d := startDriver(clk, time.Millisecond)
	defer d.Stop()

	const runs = 12
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Sampler watches the global gauge for sanity while runs are active.
	maxSeen := 0
	var maxMu sync.Mutex
	stopSampler := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopSampler:
				return
			default:
				v := e.InFlight()
				maxMu.Lock()
				if v > maxSeen {
					maxSeen = v
				}
				maxMu.Unlock()
			}
		}
	}()

	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.Run(ctx, testReq(), &captureSink{})
		}()
	}
	time.Sleep(150 * time.Millisecond)
	cancel()
	wg.Wait()
	close(stopSampler)

	// Each run starts at most 3; with 12 concurrent runs the global ceiling is
	// runs*MaxInFlight=36. The real correctness check is -race + no negative
	// inflight; this bound just guards against accounting blowups.
	maxMu.Lock()
	defer maxMu.Unlock()
	if maxSeen > runs*pol.MaxInFlight {
		t.Errorf("global inFlight peaked at %d, exceeds runs*cap=%d", maxSeen, runs*pol.MaxInFlight)
	}
	if final := e.InFlight(); final != 0 {
		t.Errorf("inFlight should return to 0 after all runs, got %d", final)
	}
}

// Client cancellation BEFORE any winner appears → the start loop's
// parentCtx.Done() case fires, all started backends are cancelled, and the
// engine returns promptly with a context error.
func TestClientCancelBeforeWinner(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Backends never produce a usable token in test time.
	mk := func(name string) *backend.FakeBackend {
		return &backend.FakeBackend{
			BackendName: name, Clock: clk,
			FirstTokenDelay: time.Hour, Tokens: []string{"x"},
		}
	}
	pol := policy.HedgePolicy{FireAfter: 5 * time.Millisecond, MaxInFlight: 3}
	e := NewEngine([]backend.Backend{mk("a"), mk("b"), mk("c")}, pol, clk)

	d := startDriver(clk, 2*time.Millisecond)
	defer d.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	sink := &captureSink{}
	type res struct {
		o   Outcome
		err error
	}
	ch := make(chan res, 1)
	go func() {
		o, err := e.Run(ctx, testReq(), sink)
		ch <- res{o, err}
	}()

	// Let some backups start, then cancel the client.
	time.Sleep(60 * time.Millisecond)
	cancel()

	select {
	case r := <-ch:
		if r.err == nil {
			t.Fatal("expected context error on client cancel before winner")
		}
		if committed, _, _ := sink.snapshot(); committed {
			t.Error("sink must not be committed when cancelled before a winner")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("engine did not return promptly after client cancel")
	}
	// After return, inFlight must be back to 0 (all torn down).
	if got := e.InFlight(); got != 0 {
		t.Errorf("inFlight=%d after cancel, want 0", got)
	}
}

// Client cancellation MID-STREAM cancels all backends and unblocks the relay.
func TestClientCancelMidStream(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Primary wins quickly, then streams slowly forever; client cancels mid-way.
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		InterTokenDelay: 10 * time.Millisecond,
		Tokens:          makeTokens(1000), // effectively endless in test time
	}
	pol := policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}
	e := NewEngine([]backend.Backend{primary}, pol, clk)

	d := startDriver(clk, 2*time.Millisecond)
	defer d.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	sink := &captureSink{}
	type res struct {
		o   Outcome
		err error
	}
	ch := make(chan res, 1)
	go func() {
		o, err := e.Run(ctx, testReq(), sink)
		ch <- res{o, err}
	}()

	// Wait until at least committed + streaming, then cancel.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if committed, _, _ := sink.snapshot(); committed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never committed")
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()

	select {
	case r := <-ch:
		if r.err == nil {
			t.Fatal("expected context error on mid-stream cancel")
		}
		if r.o.Winner != "primary" {
			t.Errorf("winner=%q want primary", r.o.Winner)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("engine did not return after mid-stream cancel (relay blocked?)")
	}
	if got := e.InFlight(); got != 0 {
		t.Errorf("inFlight=%d after cancel, want 0", got)
	}
}

// A loser blocked on channel-send is unblocked by cancel and drains (no leak).
// The backup wins; the primary, having produced a usable token into a small
// buffer that the engine stops reading, must still tear down on cancel.
func TestLoserBlockedOnSendUnblockedByCancel(t *testing.T) {
	baseline := waitGoroutines(t, 0) // capture baseline

	clk := clock.NewFakeClock(time.Time{})
	// Both backends produce tokens; whichever the engine selects first wins,
	// the other becomes a loser that may be mid-send when cancelled.
	a := &backend.FakeBackend{
		BackendName: "a", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		InterTokenDelay: 2 * time.Millisecond,
		Tokens:          makeTokens(50),
	}
	b := &backend.FakeBackend{
		BackendName: "b", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		InterTokenDelay: 2 * time.Millisecond,
		Tokens:          makeTokens(50),
	}
	// Fire both immediately so they race and one becomes a streaming loser.
	pol := policy.HedgePolicy{FireAfter: time.Millisecond, MaxInFlight: 2}
	e := NewEngine([]backend.Backend{a, b}, pol, clk)

	d := startDriver(clk, time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if o.Winner == "" {
		t.Fatal("expected a winner")
	}
	// No leak: goroutines return to baseline.
	assertNoLeak(t, baseline)
}

// Headers are NOT committed until a winner: a failing primary before a backup
// win leaves the response uncommitted up to the moment the backup wins.
func TestHeadersNotCommittedUntilWinner(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Primary loses (no usable token); backup wins later. The sink must not be
	// committed before the backup's first usable token.
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond, Tokens: nil, EmitFinish: true,
	}
	backup := &backend.FakeBackend{
		BackendName: "backup", Clock: clk,
		FirstTokenDelay: 30 * time.Millisecond,
		Tokens:          []string{"B"}, EmitFinish: true,
	}
	pol := policy.HedgePolicy{FireAfter: time.Hour, MaxInFlight: 2} // backup starts only via primary loss
	e := NewEngine([]backend.Backend{primary, backup}, pol, clk)

	d := startDriver(clk, time.Millisecond)
	defer d.Stop()

	// Observe that the sink stays uncommitted until the backup produces.
	sink := &probeSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if o.Winner != "backup" {
		t.Errorf("winner=%q want backup", o.Winner)
	}
	if !sink.committed {
		t.Error("sink should be committed once the backup won")
	}
	// The first committed content must be the backup's, never the (lost)
	// primary's (which had no usable token anyway).
	if sink.firstContent != "B" {
		t.Errorf("first committed content=%q want B", sink.firstContent)
	}
}

// A client write error mid-relay is propagated (response already committed).
func TestRelayWriteErrorPropagates(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		InterTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"A", "B", "C"}, EmitFinish: true,
	}
	pol := policy.HedgePolicy{FireAfter: time.Second, MaxInFlight: 1}
	e := NewEngine([]backend.Backend{primary}, pol, clk)

	d := startDriver(clk, time.Millisecond)
	defer d.Stop()

	// Fail on the 2nd Chunk call (the first is via Commit).
	sink := &captureSink{failOn: 2}
	_, err := runEngine(t, e, context.Background(), sink)
	if err == nil {
		t.Fatal("expected the simulated client write error to propagate")
	}
	if got := e.InFlight(); got != 0 {
		t.Errorf("inFlight=%d after write error, want 0", got)
	}
}

// Loser chunks must NEVER be forwarded to the client, even when a loser is
// actively streaming distinctive content at the moment the winner is chosen.
// Run many iterations to shake out any ordering where a loser event is dequeued
// right after Once.Do.
func TestLoserContentNeverRelayed(t *testing.T) {
	for iter := 0; iter < 25; iter++ {
		clk := clock.NewFakeClock(time.Time{})
		// Winner emits only "WIN" tokens; loser emits only "LOSE" tokens. Both
		// start immediately (fire_after tiny) and stream fast.
		winner := &backend.FakeBackend{
			BackendName: "winner", Clock: clk,
			FirstTokenDelay: 3 * time.Millisecond,
			InterTokenDelay: time.Millisecond,
			Tokens:          repeat("WIN", 100),
		}
		loser := &backend.FakeBackend{
			BackendName: "loser", Clock: clk,
			FirstTokenDelay: 3 * time.Millisecond,
			InterTokenDelay: time.Millisecond,
			Tokens:          repeat("LOSE", 100),
		}
		pol := policy.HedgePolicy{FireAfter: time.Millisecond, MaxInFlight: 2}
		e := NewEngine([]backend.Backend{winner, loser}, pol, clk)

		d := startDriver(clk, time.Millisecond)
		sink := &captureSink{}
		o, err := runEngine(t, e, context.Background(), sink)
		d.Stop()
		if err != nil {
			t.Fatalf("iter %d: err=%v", iter, err)
		}
		_, win, contents := sink.snapshot()
		for _, c := range contents {
			// Every relayed token must belong to the actual winner.
			if win == "winner" && c != "WIN" {
				t.Fatalf("iter %d: winner=%q but relayed loser token %q", iter, win, c)
			}
			if win == "loser" && c != "LOSE" {
				t.Fatalf("iter %d: winner=%q but relayed other token %q", iter, win, c)
			}
		}
		_ = o
	}
}

func TestNoBackendsError(t *testing.T) {
	e := NewEngine(nil, policy.DefaultPolicy(), clock.NewFakeClock(time.Time{}))
	_, err := e.Run(context.Background(), testReq(), &captureSink{})
	if !errors.Is(err, ErrNoBackends) {
		t.Fatalf("err=%v want ErrNoBackends", err)
	}
}

// A winner committing must cancel every LOSER backend's context IMMEDIATELY
// (not at Run return): the loser's ctx is observed cancelled while the winner is
// still streaming. This is the cost/latency point of hedging — losers must stop
// feeding the fan-in the moment a winner is chosen.
func TestLosersCancelledImmediatelyOnWinnerCommit(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})

	loserCancelled := make(chan struct{})
	// Winner wins fast then streams many tokens slowly, so its stream stays open
	// for a long (clock-driven) window after commit.
	winner := &backend.FakeBackend{
		BackendName: "winner", Clock: clk,
		FirstTokenDelay: 5 * time.Millisecond,
		InterTokenDelay: 5 * time.Millisecond,
		Tokens:          repeat("WIN", 200), // long stream in test time
		EmitFinish:      true,
	}
	// Loser never produces a usable token; it must be torn down on winner commit.
	loser := &backend.FakeBackend{
		BackendName: "loser", Clock: clk,
		FirstTokenDelay: time.Hour, // never produces in test time
		Tokens:          []string{"LOSE"},
		OnCancel:        func() { close(loserCancelled) },
	}
	// Both start immediately so the loser is in flight when the winner commits.
	pol := policy.HedgePolicy{FireAfter: time.Millisecond, MaxInFlight: 2}
	e := NewEngine([]backend.Backend{winner, loser}, pol, clk)

	d := startDriver(clk, time.Millisecond)
	defer d.Stop()

	sink := &commitSignalSink{committed: make(chan struct{})}
	type res struct {
		o   Outcome
		err error
	}
	done := make(chan res, 1)
	go func() {
		o, err := e.Run(context.Background(), testReq(), sink)
		done <- res{o, err}
	}()

	// Wait until the winner has committed (it is now streaming the rest).
	select {
	case <-sink.committed:
	case <-time.After(3 * time.Second):
		t.Fatal("winner never committed")
	}

	// The loser's context must be cancelled promptly — and crucially BEFORE the
	// run returns (i.e. while the winner is still streaming its 200 tokens).
	select {
	case <-loserCancelled:
		// good: loser torn down at winner commit
	case <-done:
		t.Fatal("loser was not cancelled until Run returned (losers kept running)")
	case <-time.After(3 * time.Second):
		t.Fatal("loser context was never cancelled")
	}

	// The run must still be in progress (winner streaming) at the moment the
	// loser was cancelled — proving immediacy, not teardown-at-return.
	select {
	case r := <-done:
		t.Fatalf("Run already returned when loser was cancelled (winner=%q); losers must be cancelled mid-stream", r.o.Winner)
	default:
		// expected: winner still streaming
	}

	// Drain the run to completion.
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("err=%v", r.err)
		}
		if r.o.Winner != "winner" {
			t.Errorf("winner=%q want winner", r.o.Winner)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not complete")
	}
	if got := e.InFlight(); got != 0 {
		t.Errorf("inFlight=%d after run, want 0", got)
	}
}

// A synchronous Stream error must roll back THIS backend's in-flight/cost
// reservation under the engine mutex, so a later backend can still start within
// the same ceiling (the failed start does not permanently consume a slot).
func TestStartErrorRollsBackReservation(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Primary errors on Stream (rolls back its reservation). Backup must then be
	// able to start within a tight cost ceiling that only has room for one.
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk, Cost: 1,
		StartErr: errors.New("connect failed"),
	}
	backup := &backend.FakeBackend{
		BackendName: "backup", Clock: clk, Cost: 1,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"B"}, EmitFinish: true,
	}
	// Ceiling 1.0 admits exactly ONE backend's cost. If the failed primary's
	// reservation were NOT rolled back, committedCost would sit at 1.0 and the
	// backup (→2.0) would be blocked → all-fail. With rollback, the backup starts
	// and wins.
	pol := policy.HedgePolicy{FireAfter: time.Hour, MaxInFlight: 5, CostCeiling: 1.0}
	e := NewEngine([]backend.Backend{primary, backup}, pol, clk)

	d := startDriver(clk, time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if err != nil {
		t.Fatalf("err=%v (backup should start after the primary start-error reservation is rolled back)", err)
	}
	if o.Winner != "backup" {
		t.Errorf("winner=%q want backup", o.Winner)
	}
	if got := e.InFlight(); got != 0 {
		t.Errorf("inFlight=%d after run, want 0", got)
	}
}

// A synchronously-failing primary followed by a backup that wins must NOT be
// counted as "started": Outcome.Started reflects only backends that issued a
// real upstream request, so the failed primary is excluded. Consequently
// RedundantStarts() is 0 (one real request, the winning backup) and the
// hedge_redundant_requests_total metric — fed exactly as the proxy feeds it —
// records 0 redundant spend on this rollback path.
func TestStartErrorNotCountedInStartedOrRedundant(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Primary errors synchronously on Stream (never issues an upstream request).
	primary := &backend.FakeBackend{
		BackendName: "primary", Clock: clk, Cost: 1,
		StartErr: errors.New("connect failed"),
	}
	// Backup starts (the only real upstream request) and wins.
	backup := &backend.FakeBackend{
		BackendName: "backup", Clock: clk, Cost: 1,
		FirstTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"B1", "B2"}, EmitFinish: true,
	}
	pol := policy.HedgePolicy{FireAfter: time.Hour, MaxInFlight: 5}
	e := NewEngine([]backend.Backend{primary, backup}, pol, clk)

	d := startDriver(clk, time.Millisecond)
	defer d.Stop()

	sink := &captureSink{}
	o, err := runEngine(t, e, context.Background(), sink)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if o.Winner != "backup" {
		t.Errorf("winner=%q want backup", o.Winner)
	}
	// The failed primary must NOT inflate Started: only the backup truly started.
	if o.Started != 1 {
		t.Errorf("Started=%d want 1 (failed primary excluded; only the backup started)", o.Started)
	}
	// One real upstream request → zero redundant starts.
	if o.RedundantStarts() != 0 {
		t.Errorf("RedundantStarts=%d want 0 (a synchronously-failed start is not a real request)", o.RedundantStarts())
	}

	// And the metric reflects it, fed exactly as proxy.Handler.report does.
	reg := metrics.NewRegistry(nil)
	reg.AddRedundantRequests(o.RedundantStarts())
	if got := scrapeMetric(t, reg, "hedge_redundant_requests_total"); got != "0" {
		t.Errorf("hedge_redundant_requests_total=%q want 0", got)
	}

	if got := e.InFlight(); got != 0 {
		t.Errorf("inFlight=%d after run, want 0", got)
	}
}

// A single-backend config whose only backend errors on start must return
// ErrAllBackendsFailed PROMPTLY (no winner, nothing producing, nothing left to
// start) — never hang until client cancel.
func TestSingleBackendStartErrorReturnsPromptly(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	only := &backend.FakeBackend{
		BackendName: "only", Clock: clk,
		StartErr: errors.New("connect failed"),
	}
	pol := policy.HedgePolicy{FireAfter: time.Hour, MaxInFlight: 1}
	e := NewEngine([]backend.Backend{only}, pol, clk)

	d := startDriver(clk, time.Millisecond)
	defer d.Stop()

	// Background context that is NEVER cancelled: the engine must terminate on
	// its own via the terminal no-path-to-winner check, not by client cancel.
	sink := &captureSink{}
	type res struct {
		o   Outcome
		err error
	}
	done := make(chan res, 1)
	go func() {
		o, err := e.Run(context.Background(), testReq(), sink)
		done <- res{o, err}
	}()

	select {
	case r := <-done:
		if !errors.Is(r.err, ErrAllBackendsFailed) {
			t.Fatalf("err=%v want ErrAllBackendsFailed", r.err)
		}
		if r.o.Winner != "" {
			t.Errorf("winner=%q want empty", r.o.Winner)
		}
		if committed, _, _ := sink.snapshot(); committed {
			t.Error("sink must not be committed when the only backend fails to start")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("engine hung instead of returning ErrAllBackendsFailed promptly")
	}
	if got := e.InFlight(); got != 0 {
		t.Errorf("inFlight=%d after run, want 0", got)
	}
}

// Issue #2: the engine consults a WithFireAfterFunc option once per Run to
// derive the fire-after delay, so a seeded adaptive estimator's p50 — not the
// static policy.FireAfter — times the backup start. Below the estimator's
// min_samples the suggestion falls back to the static value, and with no
// function installed adaptive timing is off entirely.
//
// The scenario is chosen so the fire-after delay alone decides the winner: the
// primary is slow (200ms first token) while a backup that starts EARLY (at the
// 20ms p50) produces at 30ms and wins, but a backup gated on the huge static
// delay never starts and the slow primary wins.
func TestFireAfterFuncDrivesBackupStart(t *testing.T) {
	const (
		staticFireAfter = time.Hour // static value → backup would never fire in test time
		p50             = 20 * time.Millisecond
		minSamples      = 5
	)
	newBackends := func(clk *clock.FakeClock) []backend.Backend {
		primary := &backend.FakeBackend{
			BackendName: "primary", Clock: clk,
			FirstTokenDelay: 200 * time.Millisecond,
			Tokens:          []string{"P"}, EmitFinish: true,
		}
		backup := &backend.FakeBackend{
			BackendName: "backup", Clock: clk,
			FirstTokenDelay: 10 * time.Millisecond,
			Tokens:          []string{"B"}, EmitFinish: true,
		}
		return []backend.Backend{primary, backup}
	}
	// A seeded estimator whose primary p50 is exactly p50 (constant samples).
	seededEstimator := func(samples int) *adaptive.Estimator {
		est := adaptive.NewEstimator(64)
		for i := 0; i < samples; i++ {
			est.Observe("primary", p50)
		}
		return est
	}
	// The static policy fire-after is huge, so ANY early backup start must come
	// from the adaptive suggestion, not the policy.
	pol := policy.HedgePolicy{FireAfter: staticFireAfter, MaxInFlight: 2}
	// Mirror exactly how cmd/hedge-llm wires the estimator into the engine.
	fireAfterFunc := func(est *adaptive.Estimator) func(string) time.Duration {
		return func(primary string) time.Duration {
			return est.SuggestFireAfter(primary, staticFireAfter, minSamples)
		}
	}

	t.Run("p50 times the backup start at/above min_samples", func(t *testing.T) {
		clk := clock.NewFakeClock(time.Time{})
		est := seededEstimator(minSamples + 1) // enough samples → suggestion is the p50
		e := NewEngine(newBackends(clk), pol, clk, WithFireAfterFunc(fireAfterFunc(est)))

		d := startDriver(clk, 2*time.Millisecond)
		defer d.Stop()

		o, err := runEngine(t, e, context.Background(), &captureSink{})
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		// The backup fired at the p50 (20ms) and produced (30ms) before the
		// primary's 200ms first token → backup wins and TWO backends started.
		if o.Winner != "backup" {
			t.Errorf("winner=%q want backup (backup must fire at the adaptive p50, not the static value)", o.Winner)
		}
		if o.Started != 2 {
			t.Errorf("started=%d want 2 (adaptive fire-after started the backup)", o.Started)
		}
	})

	t.Run("below min_samples falls back to the static fire_after", func(t *testing.T) {
		clk := clock.NewFakeClock(time.Time{})
		est := seededEstimator(minSamples - 3) // too few samples → suggestion is the static value
		e := NewEngine(newBackends(clk), pol, clk, WithFireAfterFunc(fireAfterFunc(est)))

		d := startDriver(clk, 2*time.Millisecond)
		defer d.Stop()

		o, err := runEngine(t, e, context.Background(), &captureSink{})
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		// With the static (huge) fire-after the backup never starts; the primary's
		// 200ms token wins and only ONE backend started.
		if o.Winner != "primary" {
			t.Errorf("winner=%q want primary (below min_samples must use the static fire_after)", o.Winner)
		}
		if o.Started != 1 {
			t.Errorf("started=%d want 1 (static fire_after must not start the backup)", o.Started)
		}
	})

	t.Run("adaptive off by default (no fire-after func)", func(t *testing.T) {
		clk := clock.NewFakeClock(time.Time{})
		// A fully-seeded estimator exists but is NOT wired into the engine, so its
		// p50 must be ignored and the static fire_after governs.
		_ = seededEstimator(minSamples + 1)
		e := NewEngine(newBackends(clk), pol, clk) // no WithFireAfterFunc

		d := startDriver(clk, 2*time.Millisecond)
		defer d.Stop()

		o, err := runEngine(t, e, context.Background(), &captureSink{})
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if o.Winner != "primary" || o.Started != 1 {
			t.Errorf("winner=%q started=%d want primary/1 (adaptive must be off by default)", o.Winner, o.Started)
		}
	})
}

func TestOutcomeHelpers(t *testing.T) {
	o := Outcome{Started: 3, FirstTokenLatency: 10 * time.Millisecond, PrimaryFirstToken: 25 * time.Millisecond}
	if o.RedundantStarts() != 2 {
		t.Errorf("RedundantStarts=%d want 2", o.RedundantStarts())
	}
	if o.LatencySaved() != 15*time.Millisecond {
		t.Errorf("LatencySaved=%v want 15ms", o.LatencySaved())
	}
	o2 := Outcome{Started: 1}
	if o2.RedundantStarts() != 0 || o2.LatencySaved() != 0 {
		t.Errorf("single-backend outcome helpers wrong: %+v", o2)
	}
}

// ---- leak-assertion helper -------------------------------------------------

// waitGoroutines lets transient goroutines settle and returns a baseline count.
func waitGoroutines(t *testing.T, _ int) int {
	t.Helper()
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	return runtime.NumGoroutine()
}

// assertNoLeak polls NumGoroutine back toward the baseline with a bounded
// retry/settle loop, tolerating runtime/timer/race-detector goroutines. It
// fails only if the count stays meaningfully elevated.
func assertNoLeak(t *testing.T, baseline int) {
	t.Helper()
	const (
		tolerance = 2 // allow a couple of runtime/timer goroutines
		attempts  = 50
	)
	var last int
	for i := 0; i < attempts; i++ {
		runtime.Gosched()
		runtime.GC()
		last = runtime.NumGoroutine()
		if last <= baseline+tolerance {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: baseline=%d current=%d (tolerance=%d)", baseline, last, tolerance)
}

// scrapeMetric renders the registry's Prometheus exposition and returns the
// value of the first unlabelled sample line for the named metric (e.g.
// "hedge_redundant_requests_total 0" → "0"). It fails if the metric is absent.
func scrapeMetric(t *testing.T, reg *metrics.Registry, name string) string {
	t.Helper()
	var sb strings.Builder
	if _, err := reg.WriteTo(&sb); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	sc := bufio.NewScanner(strings.NewReader(sb.String()))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue // HELP/TYPE comment
		}
		// Match exactly "<name> <value>" (no label braces).
		if v, ok := strings.CutPrefix(line, name+" "); ok {
			return strings.TrimSpace(v)
		}
	}
	t.Fatalf("metric %q not found in exposition:\n%s", name, sb.String())
	return ""
}

func makeTokens(n int) []string {
	return repeat("tok", n)
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// commitSignalSink closes committed on the first Commit so a test can observe
// the exact moment the winner is chosen while the relay continues. Subsequent
// chunks are accepted and discarded.
type commitSignalSink struct {
	committed chan struct{}
	once      sync.Once
}

func (s *commitSignalSink) Commit(_ string, first oapi.Chunk) error {
	s.once.Do(func() { close(s.committed) })
	return s.Chunk(first)
}

func (s *commitSignalSink) Chunk(_ oapi.Chunk) error { return nil }

// probeSink records the first committed content and whether commit happened.
type probeSink struct {
	mu           sync.Mutex
	committed    bool
	firstContent string
}

func (s *probeSink) Commit(_ string, first oapi.Chunk) error {
	s.mu.Lock()
	s.committed = true
	s.mu.Unlock()
	return s.Chunk(first)
}

func (s *probeSink) Chunk(c oapi.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstContent == "" {
		if u := c.UsableContent(); u != "" {
			s.firstContent = u
		}
	}
	return nil
}
