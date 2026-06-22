// Package adaptive maintains per-backend rolling first-token latency statistics
// and derives a suggested hedge fire-after delay from them.
//
// Adaptive timing is off by default (the engine uses the static policy
// FireAfter); it is opt-in. Statistics are kept in memory only (documented
// non-goal: no persistence). Memory is bounded: each backend keeps at most a
// fixed window of recent samples in a ring buffer.
package adaptive

import (
	"sort"
	"sync"
	"time"
)

// DefaultWindow is the number of recent first-token latency samples retained
// per backend when none is specified.
const DefaultWindow = 128

// ring is a fixed-capacity ring buffer of latency samples for one backend.
//
// All access is guarded by the enclosing Estimator's mutex. Element writes and
// the index update happen together under that lock — there is no
// atomic-index-with-racy-element pattern.
type ring struct {
	buf   []time.Duration
	next  int
	count int
}

func newRing(capacity int) *ring {
	return &ring{buf: make([]time.Duration, capacity)}
}

func (r *ring) add(d time.Duration) {
	r.buf[r.next] = d
	r.next = (r.next + 1) % len(r.buf)
	if r.count < len(r.buf) {
		r.count++
	}
}

// snapshot returns a sorted copy of the currently-held samples.
//
// buf[:count] captures exactly the live samples in BOTH ring states, and the
// result is sorted so the physical slot order never matters:
//
//   - Pre-wrap (count < len(buf)): writes have only ever landed in slots
//     0..next-1 and next == count, so buf[:count] is precisely the populated
//     prefix; the unused tail (buf[count:], still zero values) is excluded.
//   - Post-wrap (count == len(buf)): the buffer is full, so buf[:count] is the
//     whole buffer — every slot holds a real sample. The oldest/newest split at
//     index next is irrelevant here because we sort the copy before returning.
func (r *ring) snapshot() []time.Duration {
	if r.count == 0 {
		return nil
	}
	out := make([]time.Duration, r.count)
	copy(out, r.buf[:r.count])
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Estimator tracks rolling first-token latencies per backend and suggests a
// fire-after delay. It is safe for concurrent use: samples may arrive from many
// in-flight requests while the policy reads suggestions.
type Estimator struct {
	mu     sync.Mutex
	window int
	rings  map[string]*ring
}

// NewEstimator creates an Estimator retaining the given number of samples per
// backend. A non-positive window falls back to DefaultWindow.
func NewEstimator(window int) *Estimator {
	if window <= 0 {
		window = DefaultWindow
	}
	return &Estimator{window: window, rings: make(map[string]*ring)}
}

// Observe records a first-token latency sample for a backend.
func (e *Estimator) Observe(backend string, firstToken time.Duration) {
	if firstToken < 0 {
		firstToken = 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	r := e.rings[backend]
	if r == nil {
		r = newRing(e.window)
		e.rings[backend] = r
	}
	r.add(firstToken)
}

// Percentile returns the q-th percentile (0..1) of the backend's recent
// first-token latencies, and ok=false if there are no samples yet. q is clamped
// to [0,1]. Uses nearest-rank on the sorted sample set.
func (e *Estimator) Percentile(backend string, q float64) (time.Duration, bool) {
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	e.mu.Lock()
	r := e.rings[backend]
	var samples []time.Duration
	if r != nil {
		samples = r.snapshot()
	}
	e.mu.Unlock()

	if len(samples) == 0 {
		return 0, false
	}
	// Nearest-rank: rank in [1, n].
	rank := int(q*float64(len(samples)-1) + 0.5)
	if rank < 0 {
		rank = 0
	}
	if rank >= len(samples) {
		rank = len(samples) - 1
	}
	return samples[rank], true
}

// SuggestFireAfter returns a suggested hedge delay for the primary backend
// derived from its recent latency distribution (its p50 by default), falling
// back to defaultDelay when there are not yet enough samples (fewer than
// minSamples). This lets the engine wait roughly as long as the primary
// usually takes to produce a first token before hedging.
func (e *Estimator) SuggestFireAfter(primary string, defaultDelay time.Duration, minSamples int) time.Duration {
	if minSamples < 1 {
		minSamples = 1
	}
	e.mu.Lock()
	r := e.rings[primary]
	n := 0
	if r != nil {
		n = r.count
	}
	e.mu.Unlock()

	if n < minSamples {
		return defaultDelay
	}
	if p50, ok := e.Percentile(primary, 0.5); ok {
		return p50
	}
	return defaultDelay
}
