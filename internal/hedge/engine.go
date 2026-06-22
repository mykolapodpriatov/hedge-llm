// Package hedge implements the core hedging race: start a primary backend,
// fire speculative backups per the policy, stream the first backend to emit a
// usable token, and cancel the losers — with no goroutine or connection leaks.
//
// The concurrency invariants enforced here are the project's reason to exist;
// they are exercised directly under `go test -race`:
//
//  1. Cancel-before-wait: the engine cancels all child contexts FIRST, then
//     WaitGroup.Wait()s, so a backend blocked on a network read is torn down
//     before the drain. The HTTP handler calls Engine.Run, which performs this
//     drain before returning, so http.Server.Shutdown cannot return while
//     backend goroutines are still draining. Moreover each backend has its OWN
//     child context: the instant a winner commits, every LOSER's context is
//     cancelled immediately (not at Run return), so losing upstreams are torn
//     down at once and stop feeding the fan-in; only the winner keeps its
//     context until its relay finishes.
//  2. Pre-winner racing select watches a one-shot, re-armed fire-after timer
//     (never a Ticker), backend first-token/loss events, and parentCtx.Done().
//  3. Atomic check-and-increment of inFlight + committed cost under ONE mutex
//     (no TOCTOU on max_in_flight / cost_ceiling).
//  4. Winner selected via sync.Once; afterwards the relay reads ONLY the
//     winner's events and clientCtx.Done() — loser events are discarded.
//  5. A backend that closes with no usable token is a LOSS, not a hang;
//     all-lose returns an error.
//  6. Header-commit happens only after a winner (the Sink is told to commit on
//     the first usable token), so an all-fail case leaves the response
//     uncommitted for a clean error.
//
// # Channel reader discipline
//
// Each started backend has exactly ONE reader: a dedicated forwarder goroutine
// that drains the backend's chunk channel and pushes (index, chunk, open)
// events onto a single shared fan-in channel using the cancellation-safe send
// pattern. The engine never reads a backend channel directly, so there is no
// double-reader race and no chunk can be "stolen". After a winner is chosen the
// engine keeps draining the shared fan-in channel but forwards ONLY the
// winner's events to the sink; loser forwarders observe runCtx cancellation and
// exit, and the WaitGroup tracks every forwarder so Run drains them all before
// returning.
package hedge

import (
	"context"
	"errors"
	"sync"
	"time"

	"hedge-llm/internal/backend"
	"hedge-llm/internal/clock"
	"hedge-llm/internal/oapi"
	"hedge-llm/internal/policy"
)

// ErrAllBackendsFailed is returned by Run when every backend lost (errored,
// produced no usable token, or was otherwise unable to win) and no winner was
// committed.
var ErrAllBackendsFailed = errors.New("hedge-llm: all backends failed to produce a usable token")

// ErrNoBackends is returned by Run when no backends were supplied.
var ErrNoBackends = errors.New("hedge-llm: no backends configured")

// Sink consumes the winning backend's stream. The engine calls Commit exactly
// once — when the first usable token arrives — and only then is it safe for the
// proxy to write response headers/200. After Commit, the engine calls Chunk for
// every relayed chunk of the winner (including the committing chunk).
//
// Implementations must be cheap and non-blocking relative to the client; a slow
// or gone client is surfaced to the engine via the client context passed to
// Run, which unblocks the relay.
type Sink interface {
	// Commit is called once, with the winning backend's name and its first
	// usable chunk, immediately before the first Chunk call. The proxy commits
	// response headers here.
	Commit(backend string, first oapi.Chunk) error
	// Chunk is called for each chunk of the winner's stream, starting with the
	// chunk passed to Commit. Returning an error stops the relay (e.g. the
	// client write failed).
	Chunk(c oapi.Chunk) error
}

// Outcome summarises a completed hedge run for metrics/inspection.
type Outcome struct {
	// Winner is the name of the backend that won, or "" if none did.
	Winner string
	// Started is how many backends actually issued an upstream request — i.e.
	// were started successfully. A backend whose Stream() failed synchronously
	// never issued a request and is NOT counted, so this (and RedundantStarts)
	// reflects only real upstream spend.
	Started int
	// FirstTokenLatency is the elapsed time from Run start to the winner's
	// first usable token.
	FirstTokenLatency time.Duration
	// PrimaryFirstToken, when non-zero, is the primary's own first-usable-token
	// latency (used to estimate latency saved when a backup won).
	PrimaryFirstToken time.Duration
}

// RedundantStarts is the number of speculative backups started beyond the
// primary.
func (o Outcome) RedundantStarts() int {
	if o.Started <= 1 {
		return 0
	}
	return o.Started - 1
}

// LatencySaved estimates the first-token latency saved by hedging: when a backup
// won and the primary's own first token was observed (or estimated) to be
// slower, the saving is the difference. Returns 0 when the primary won or no
// estimate is available.
func (o Outcome) LatencySaved() time.Duration {
	if o.PrimaryFirstToken > o.FirstTokenLatency {
		return o.PrimaryFirstToken - o.FirstTokenLatency
	}
	return 0
}

// Engine runs hedged requests across a fixed ordered set of backends.
type Engine struct {
	backends []backend.Backend
	pol      policy.HedgePolicy
	clk      clock.Clock

	// mu guards inFlight + committedCost. The start decision is a
	// check-and-increment under this single lock so two goroutines can never
	// both observe headroom and both start (no TOCTOU on the bounds).
	mu            sync.Mutex
	inFlight      int
	committedCost float64
}

// NewEngine constructs an Engine. clk defaults to clock.RealClock if nil.
func NewEngine(backends []backend.Backend, pol policy.HedgePolicy, clk clock.Clock) *Engine {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return &Engine{backends: backends, pol: pol, clk: clk}
}

// InFlight returns the current number of in-flight backends across the engine,
// read under the same mutex that guards the start decision. This is the single
// source of truth for the hedge_inflight metric gauge.
func (e *Engine) InFlight() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.inFlight
}

// event is a fan-in message from a backend forwarder.
type event struct {
	idx   int        // index into the run's started slice
	chunk oapi.Chunk // valid only when open is true
	open  bool       // false signals the backend channel closed
}

// backendState tracks one started backend during a run.
type backendState struct {
	be      backend.Backend
	cancel  context.CancelFunc // cancels ONLY this backend's context
	firstAt time.Duration      // first-usable-token latency, 0 until/unless seen
	lost    bool               // channel closed with no usable token
	closed  bool               // channel fully closed
	failed  bool               // Stream() returned an error synchronously (reservation rolled back)
}

// run holds the mutable state of a single Run invocation.
type run struct {
	e        *Engine
	runCtx   context.Context
	req      *oapi.Request
	start    time.Time
	wg       *sync.WaitGroup
	events   chan event
	states   []*backendState
	nextIdx  int
	reserved int // backends this run reserved against the engine's inFlight
}

// Run executes one hedged request. It blocks until a winner has been fully
// relayed to the sink, until all backends have lost, or until clientCtx is
// cancelled — and only returns AFTER cancelling every child context and waiting
// for all backend goroutines to drain (cancel-before-wait).
//
// On success Run returns the Outcome and a nil error. If no backend produces a
// usable token it returns ErrAllBackendsFailed. If clientCtx is cancelled before
// a winner it returns the context error. If the sink reports a write error
// mid-relay (response already committed), that error is returned.
func (e *Engine) Run(clientCtx context.Context, req *oapi.Request, sink Sink) (Outcome, error) {
	if len(e.backends) == 0 {
		return Outcome{}, ErrNoBackends
	}

	runCtx, cancelAll := context.WithCancel(clientCtx)
	var wg sync.WaitGroup
	r := &run{
		e:      e,
		runCtx: runCtx,
		req:    req,
		start:  e.clk.Now(),
		wg:     &wg,
		// Buffer sized len(backends)*2 so each backend can have BOTH an in-flight
		// chunk event and its final close event queued without a forwarder ever
		// blocking on a full channel — a margin that keeps a benign future change
		// (e.g. a forwarder emitting an extra event) from deadlocking. Correctness
		// still rests on the per-backend ctx.Done() select in the forwarder, not
		// on the buffer.
		events: make(chan event, len(e.backends)*2),
		states: make([]*backendState, 0, len(e.backends)),
	}

	winner, firstTok, primaryFirst, relayErr := r.race(sink)

	// Cancel-before-wait: tear down all children FIRST, then wait for every
	// forwarder (and its producer) to drain. This is the leak-free guarantee
	// and runs before Run returns to net/http.
	cancelAll()
	wg.Wait()

	// Release this run's in-flight reservations now that all backends are torn
	// down, restoring the engine-wide gauge.
	e.releaseN(r.reserved, r.committedCostDelta())

	outcome := Outcome{
		Winner:            winner,
		Started:           r.started(),
		FirstTokenLatency: firstTok,
		PrimaryFirstToken: primaryFirst,
	}
	if winner == "" && relayErr == nil {
		relayErr = ErrAllBackendsFailed
	}
	return outcome, relayErr
}

// started reports how many backends THIS run actually got an upstream request
// out for: every state whose Stream() did not fail synchronously. A backend
// whose Stream() errored never issued a request (its reservation was rolled back
// in startNext and failed=true), so counting it would inflate Outcome.Started
// and therefore RedundantStarts()/the redundant-spend metric on the rollback
// path. It excludes st.failed exactly as committedCostDelta does.
func (r *run) started() int {
	n := 0
	for _, st := range r.states {
		if st.failed {
			continue
		}
		n++
	}
	return n
}

// committedCostDelta returns the total speculative cost this run STILL holds
// (sum of CostPerRequest over started backends), used to undo the reservation at
// Run return. Backends whose Stream failed synchronously are excluded: their
// reservation was already rolled back in startNext, so counting them here would
// double-release. It mirrors r.reserved, which is likewise decremented on a
// failed start.
func (r *run) committedCostDelta() float64 {
	var c float64
	for _, st := range r.states {
		if st.failed {
			continue
		}
		c += st.be.CostPerRequest()
	}
	return c
}

// startNext attempts a policy-gated check-and-increment under the engine mutex
// and, if allowed, starts the next backend with its own per-backend context and
// forwarder goroutine. Returns true if a start was ATTEMPTED (the backend index
// is consumed either way). The primary (inFlight==0 for this run) is always
// allowed; backups go through the policy gate. A synchronous Stream error rolls
// back this backend's reservation under the same mutex and is recorded as an
// immediate loss.
func (r *run) startNext() bool {
	e := r.e
	if r.nextIdx >= len(e.backends) {
		return false
	}
	be := e.backends[r.nextIdx]

	e.mu.Lock()
	// The first backend of THIS run is the primary and always starts. For
	// backups, enforce both bounds atomically under the single lock.
	if len(r.states) > 0 && !e.pol.AllowStart(e.inFlight, e.committedCost, be.CostPerRequest()) {
		e.mu.Unlock()
		return false
	}
	e.inFlight++
	e.committedCost += be.CostPerRequest()
	e.mu.Unlock()

	r.nextIdx++
	r.reserved++

	idx := len(r.states)
	// Each backend gets its OWN context derived from runCtx so losers can be
	// cancelled the instant a winner commits, independently of the others.
	beCtx, beCancel := context.WithCancel(r.runCtx)
	st := &backendState{be: be, cancel: beCancel}
	r.states = append(r.states, st)

	ch, err := be.Stream(beCtx, r.req)
	if err != nil {
		// Could not even start. Roll back THIS backend's reservation under the
		// same mutex right now (so a later backend can still start within the
		// ceiling), and record an immediate loss. failed=true keeps it out of the
		// en-masse release in Run so the reservation is not double-counted.
		beCancel()
		st.failed = true
		st.lost = true
		st.closed = true
		r.reserved--
		e.releaseN(1, be.CostPerRequest())
		// Emit a synthetic close event so the race loop processes the loss
		// uniformly.
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			select {
			case r.events <- event{idx: idx, open: false}:
			case <-r.runCtx.Done():
			}
		}()
		return true
	}

	r.wg.Add(1)
	go r.forward(idx, beCtx, ch)
	return true
}

// forward is the SOLE reader of one backend's chunk channel. It pushes each
// chunk and the final close onto the shared fan-in channel using the
// cancellation-safe send pattern, so a cancelled run never blocks the forwarder.
// beCtx is THIS backend's own context: when it is cancelled (because this
// backend lost and the winner committed, or because the whole run is tearing
// down) the forwarder drains the backend channel to completion so the producer's
// deferred close/Body.Close runs and no goroutine leaks. Sends onto the shared
// fan-in still guard on runCtx.Done() so a full-run teardown can never wedge a
// forwarder on a send.
func (r *run) forward(idx int, beCtx context.Context, ch <-chan oapi.Chunk) {
	defer r.wg.Done()
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				// Signal close; best-effort under cancellation.
				select {
				case r.events <- event{idx: idx, open: false}:
				case <-r.runCtx.Done():
				}
				return
			}
			select {
			case r.events <- event{idx: idx, chunk: c, open: true}:
			case <-beCtx.Done():
				// This backend was cancelled (lost + winner committed, or run
				// teardown). Drain the rest so its producer closes, then exit. No
				// further chunk is forwarded, so a loser's content never reaches
				// the client.
				for range ch {
				}
				return
			}
		case <-beCtx.Done():
			// Cancelled before this backend produced anything more. Drain to let
			// the producer's deferred close/Body.Close run.
			for range ch {
			}
			return
		}
	}
}

// race runs the pre-winner racing select, then the winner relay. It returns the
// winner name (empty if none), the winner's first-token latency, the primary's
// first-token latency if observed, and any relay error.
func (r *run) race(sink Sink) (winnerName string, firstTok, primaryFirst time.Duration, relayErr error) {
	e := r.e

	// Start the primary.
	r.startNext()

	// ONE re-armable fire-after timer reused for the whole race (NOT a fresh
	// timer per re-arm, which would leak abandoned *time.Timers). It starts
	// stopped/disarmed; armFire Resets it, and it is Stopped when the race ends.
	fireTimer := e.clk.NewTimer(e.pol.FireAfter)
	fireTimer.Stop()
	defer fireTimer.Stop()
	armed := false
	// armFire (re-)arms the single fire-after timer when, and only when, more
	// backends remain and the policy has in-flight headroom. The HasHeadroom call
	// here is a NON-AUTHORITATIVE hint to avoid arming a pointless timer; the real
	// gate against overshooting max_in_flight / cost_ceiling is the atomic
	// check-and-increment inside startNext, so a stale hint can only cost a
	// harmless extra timer fire that startNext then declines.
	armFire := func() {
		if e.pol.HasHeadroom(e.InFlight()) && r.hasMore() {
			fireTimer.Reset(e.pol.FireAfter)
			armed = true
		} else {
			fireTimer.Stop()
			armed = false
		}
	}
	armFire()

	var winnerOnce sync.Once
	winnerIdx := -1
	var winnerFirst oapi.Chunk

	for {
		// Terminal check: with no winner, nothing currently producing, and no
		// further backend that can be started, there is no path to a usable token.
		// Return promptly with no winner (Run maps this to ErrAllBackendsFailed)
		// instead of blocking forever on an empty events channel.
		if r.noPathToWinner() {
			return "", 0, primaryFirst, nil
		}

		select {
		case <-r.runCtx.Done():
			// parentCtx.Done(): client disconnect / shutdown. Return at once;
			// Run cancels + waits. No speculative work survives.
			return "", 0, primaryFirst, r.runCtx.Err()

		case <-fireTimer.C():
			// Timer fired: start the next backup (check-and-increment inside),
			// then re-arm the single timer.
			r.startNext()
			armFire()

		case ev := <-r.events:
			st := r.states[ev.idx]
			if !ev.open {
				st.closed = true
				if st.firstAt == 0 {
					st.lost = true
					if r.shouldStartAfterLoss() {
						r.startNext()
						armFire()
					}
				}
				if !armed {
					armFire()
				}
				continue
			}
			usable := ev.chunk.IsUsable()
			if usable && st.firstAt == 0 {
				st.firstAt = e.clk.Now().Sub(r.start)
				if ev.idx == 0 {
					primaryFirst = st.firstAt
				}
			}
			if !usable {
				// Empty/heartbeat/finish-only chunk never wins.
				continue
			}
			// First usable token → winner via sync.Once.
			won := false
			winnerOnce.Do(func() {
				won = true
				winnerIdx = ev.idx
				winnerFirst = ev.chunk
				winnerName = st.be.Name()
				firstTok = e.clk.Now().Sub(r.start)
			})
			if won {
				// Cancel every OTHER (loser) backend immediately so their
				// upstreams tear down now and they stop feeding the fan-in; the
				// winner keeps its own context until the relay finishes. A loser's
				// content is never forwarded (relay discards non-winner events).
				fireTimer.Stop()
				armed = false
				r.cancelLosers(winnerIdx)
				if err := sink.Commit(winnerName, winnerFirst); err != nil {
					return winnerName, firstTok, primaryFirst, err
				}
				relayErr = r.relay(sink, winnerIdx)
				return winnerName, firstTok, primaryFirst, relayErr
			}
		}
	}
}

// cancelLosers cancels the per-backend context of every started backend except
// the winner, so losing upstreams are torn down the instant the winner commits
// (rather than at Run return). The winner's context is left untouched so its
// relay can finish; Run's cancelAll covers the winner afterwards.
func (r *run) cancelLosers(winnerIdx int) {
	for i, st := range r.states {
		if i == winnerIdx || st.cancel == nil {
			continue
		}
		st.cancel()
	}
}

// relay forwards the rest of the winner's stream to the sink. The winner's
// FIRST chunk was already delivered by sink.Commit (which calls sink.Chunk
// internally), so relay does NOT re-emit it — it resumes with subsequent
// chunks. It keeps draining the shared fan-in channel (the SOLE place backend
// channels are read) but forwards ONLY the winner's chunks to the sink; loser
// events are discarded. A client disconnect (runCtx.Done) stops the relay
// immediately.
func (r *run) relay(sink Sink, winnerIdx int) error {
	for {
		select {
		case <-r.runCtx.Done():
			// Mid-stream client disconnect / shutdown: stop immediately. Run
			// cancels all backends and waits.
			return r.runCtx.Err()
		case ev := <-r.events:
			if ev.idx != winnerIdx {
				// Loser event arriving after the winner was chosen: discard. It
				// is NEVER forwarded to the client.
				continue
			}
			if !ev.open {
				return nil // winner stream ended cleanly
			}
			if err := sink.Chunk(ev.chunk); err != nil {
				return err
			}
		}
	}
}

// hasMore reports whether at least one configured backend has not yet started.
func (r *run) hasMore() bool { return r.nextIdx < len(r.e.backends) }

// producing reports the number of started backends whose stream has not yet
// closed — i.e. backends that could still produce a usable token.
func (r *run) producing() int {
	n := 0
	for _, st := range r.states {
		if !st.closed {
			n++
		}
	}
	return n
}

// canStartAnother reports, read-only, whether the next un-started backend could
// be started right now under the policy (in-flight headroom AND cost ceiling).
// It mirrors startNext's gate without side effects; like the other policy reads
// in this file the value is a hint (the authoritative gate is startNext's atomic
// check-and-increment), used to decide whether the race still has a path to a
// winner.
func (r *run) canStartAnother() bool {
	e := r.e
	if r.nextIdx >= len(e.backends) {
		return false
	}
	be := e.backends[r.nextIdx]
	e.mu.Lock()
	defer e.mu.Unlock()
	// The primary (this run has started nothing yet) is always startable.
	if len(r.states) == 0 {
		return true
	}
	return e.pol.AllowStart(e.inFlight, e.committedCost, be.CostPerRequest())
}

// noPathToWinner reports whether the race can no longer reach a usable token: no
// backend is currently producing AND no further backend can be started. The race
// loop uses this to return promptly (mapped to ErrAllBackendsFailed) instead of
// blocking forever on an idle events channel — covering both "all started and
// lost" and "some remain but the policy/cost ceiling forbids starting them".
func (r *run) noPathToWinner() bool {
	if len(r.states) == 0 {
		return false // primary not started yet
	}
	if r.producing() > 0 {
		return false // something might still win
	}
	return !r.canStartAnother()
}

// shouldStartAfterLoss reports whether the policy still permits starting another
// backend (headroom remains and one is left).
func (r *run) shouldStartAfterLoss() bool {
	return r.e.pol.HasHeadroom(r.e.InFlight()) && r.hasMore()
}

// releaseN decrements the engine-wide in-flight reservation by n and the
// committed cost by cost. Called once per Run after all backends are torn down.
func (e *Engine) releaseN(n int, cost float64) {
	if n <= 0 && cost == 0 {
		return
	}
	e.mu.Lock()
	e.inFlight -= n
	if e.inFlight < 0 {
		e.inFlight = 0
	}
	e.committedCost -= cost
	if e.committedCost < 0 {
		e.committedCost = 0
	}
	e.mu.Unlock()
}
