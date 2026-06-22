// Package clock provides a small Clock abstraction so that timing-sensitive
// code (the hedge engine, the fake backend) can be driven deterministically in
// tests instead of relying on real wall-clock sleeps.
//
// Production code uses [RealClock]. Tests use [FakeClock], whose timers fire
// only when the test advances time explicitly, making the hedge race fully
// deterministic and free of real sleeps.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock is the subset of time operations the rest of the codebase needs. Keeping
// the surface small makes the fake implementation easy to reason about.
type Clock interface {
	// Now returns the current time according to the clock.
	Now() time.Time
	// After returns a channel that delivers the current time once the given
	// duration has elapsed. It mirrors time.After, including the one-shot
	// semantics callers rely on.
	After(d time.Duration) <-chan time.Time
	// NewTimer returns a single, reusable [Timer] armed for d. Unlike After it
	// can be stopped and re-armed, so a caller that re-arms repeatedly (e.g. the
	// hedge engine's fire-after) can keep ONE timer instead of leaking a fresh
	// *time.Timer per re-arm.
	NewTimer(d time.Duration) Timer
}

// Timer is a single, re-armable timer. It mirrors the parts of *time.Timer the
// engine needs. The same Timer is reused across re-arms: Reset re-arms it and
// Stop disarms it, so no abandoned timers accumulate.
type Timer interface {
	// C is the channel on which the current time is delivered when the timer
	// fires. It is stable for the life of the Timer.
	C() <-chan time.Time
	// Reset re-arms the timer to fire after d relative to now, discarding any
	// pending-but-undelivered fire. It mirrors the recommended Stop+drain+Reset
	// usage so a re-armed timer never delivers a stale tick.
	Reset(d time.Duration)
	// Stop disarms the timer so it will not fire. It is safe to call more than
	// once and after the timer has already fired.
	Stop()
}

// RealClock is a [Clock] backed by the standard library's time package.
type RealClock struct{}

// Now reports the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// After delegates to time.After.
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTimer returns a [Timer] backed by a single *time.Timer.
func (RealClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

// realTimer wraps *time.Timer to satisfy [Timer].
type realTimer struct {
	t *time.Timer
}

func (r *realTimer) C() <-chan time.Time { return r.t.C }

func (r *realTimer) Reset(d time.Duration) {
	// Stop and drain any pending tick before re-arming so the channel cannot
	// deliver a stale fire after Reset (the documented safe re-arm pattern).
	if !r.t.Stop() {
		select {
		case <-r.t.C:
		default:
		}
	}
	r.t.Reset(d)
}

func (r *realTimer) Stop() {
	if !r.t.Stop() {
		select {
		case <-r.t.C:
		default:
		}
	}
}

// fakeTimer is a single pending After() request on a FakeClock.
type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

// FakeClock is a manually-advanced [Clock] for deterministic tests. Timers
// created via [FakeClock.After] or [FakeClock.NewTimer] fire only when
// [FakeClock.Advance] (or [FakeClock.Set]) moves the clock past their deadline.
// It is safe for concurrent use.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock returns a FakeClock initialized to the given time. If start is
// the zero time, an arbitrary fixed epoch is used so durations behave sensibly.
func NewFakeClock(start time.Time) *FakeClock {
	if start.IsZero() {
		start = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &FakeClock{now: start}
}

// Now reports the fake clock's current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After registers a timer that fires after d relative to the fake clock's
// current time. A non-positive duration fires immediately.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := c.now.Add(d)
	if !deadline.After(c.now) {
		// Already due: deliver immediately without registering.
		ch <- c.now
		return ch
	}
	c.timers = append(c.timers, &fakeTimer{deadline: deadline, ch: ch})
	return ch
}

// NewTimer returns a re-armable [Timer] on this fake clock. The timer owns a
// single buffered channel for its whole life; Reset re-registers the same
// channel for a new deadline (draining any pending tick first) and Stop
// deregisters it, so re-arming never leaks registrations.
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	t := &fakeClockTimer{clk: c, ch: make(chan time.Time, 1)}
	t.Reset(d)
	return t
}

// fakeClockTimer is a [Timer] driven by a FakeClock. Its single channel is
// re-registered with the clock on each Reset.
type fakeClockTimer struct {
	clk *FakeClock
	ch  chan time.Time
}

func (t *fakeClockTimer) C() <-chan time.Time { return t.ch }

func (t *fakeClockTimer) Reset(d time.Duration) {
	c := t.clk
	c.mu.Lock()
	defer c.mu.Unlock()
	// Drop any prior registration for this channel and any pending undelivered
	// tick, so the re-armed timer fires exactly once for the new deadline.
	c.removeChanLocked(t.ch)
	select {
	case <-t.ch:
	default:
	}
	deadline := c.now.Add(d)
	if !deadline.After(c.now) {
		t.ch <- c.now
		return
	}
	c.timers = append(c.timers, &fakeTimer{deadline: deadline, ch: t.ch})
}

func (t *fakeClockTimer) Stop() {
	c := t.clk
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeChanLocked(t.ch)
	select {
	case <-t.ch:
	default:
	}
}

// removeChanLocked removes any registered fakeTimer using channel ch. Caller
// must hold c.mu.
func (c *FakeClock) removeChanLocked(ch chan time.Time) {
	if len(c.timers) == 0 {
		return
	}
	kept := c.timers[:0]
	for _, ft := range c.timers {
		if ft.ch != ch {
			kept = append(kept, ft)
		}
	}
	c.timers = append([]*fakeTimer(nil), kept...)
}

// Advance moves the fake clock forward by d and fires every timer whose
// deadline is now reached, in deadline order.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.fireLocked()
	c.mu.Unlock()
}

// Set moves the fake clock to an absolute time t (must not move backwards) and
// fires every timer whose deadline is now reached.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	if t.After(c.now) {
		c.now = t
	}
	c.fireLocked()
	c.mu.Unlock()
}

// fireLocked delivers and removes all due timers. Caller must hold c.mu.
func (c *FakeClock) fireLocked() {
	if len(c.timers) == 0 {
		return
	}
	sort.Slice(c.timers, func(i, j int) bool {
		return c.timers[i].deadline.Before(c.timers[j].deadline)
	})
	remaining := c.timers[:0]
	now := c.now
	for _, t := range c.timers {
		if !t.deadline.After(now) {
			// Buffered channel (cap 1); this never blocks.
			t.ch <- now
		} else {
			remaining = append(remaining, t)
		}
	}
	// Reset the slice to the kept timers (avoid retaining fired entries).
	c.timers = append([]*fakeTimer(nil), remaining...)
}
