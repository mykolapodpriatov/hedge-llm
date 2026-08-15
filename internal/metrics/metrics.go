// Package metrics implements a small, hand-rolled Prometheus text-exposition
// registry for hedge-llm — no client_golang dependency.
//
// # Synchronization
//
//   - Scalar counters/gauges use sync/atomic 64-bit operations.
//   - The histogram (bucket slice + sum + count) is mutated under a sync.Mutex,
//     because atomics alone cannot keep the bucket slice and the sum/count
//     consistent. The /metrics read snapshots the histogram under the same lock.
//   - The hedge_inflight gauge has a single source of truth: it is read from
//     the engine's mutex-protected in-flight counter at scrape time via an
//     injected InFlightFunc, not a separately-updated atomic that could drift.
//
// The text exposition (sorted label order, the +Inf bucket, cumulative
// _bucket/_sum/_count semantics, label-value quoting) follows the Prometheus
// text format and is validated structurally in tests.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultFirstTokenBuckets are reasonable upper bounds (in seconds) for
// first-token latency. The implicit +Inf bucket is always appended on output.
var DefaultFirstTokenBuckets = []float64{
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// histogram is a cumulative-bucket latency histogram guarded by a mutex.
type histogram struct {
	mu     sync.Mutex
	bounds []float64 // sorted upper bounds, excluding +Inf
	counts []uint64  // per-bucket (non-cumulative) observation counts
	sum    float64
	count  uint64
}

func newHistogram(bounds []float64) *histogram {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	return &histogram{bounds: b, counts: make([]uint64, len(b)+1)}
}

func (h *histogram) observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Find the first bound >= v; values exceeding all bounds land in the
	// trailing (+Inf) bucket at index len(bounds).
	idx := sort.SearchFloat64s(h.bounds, v)
	h.counts[idx]++
	h.sum += v
	h.count++
}

// histSnapshot is an immutable view of a histogram for exposition.
type histSnapshot struct {
	bounds     []float64
	cumulative []uint64 // cumulative counts, length len(bounds)+1 (last is +Inf)
	sum        float64
	count      uint64
}

func (h *histogram) snapshot() histSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	cum := make([]uint64, len(h.counts))
	var running uint64
	for i, c := range h.counts {
		running += c
		cum[i] = running
	}
	return histSnapshot{
		bounds:     append([]float64(nil), h.bounds...),
		cumulative: cum,
		sum:        h.sum,
		count:      h.count,
	}
}

// lossKey identifies one per-backend loss counter series. The reason is bounded
// to the fixed hedge.LossReason* set, so cardinality stays backends × reasons.
type lossKey struct {
	backend string
	reason  string
}

// Registry holds all hedge-llm metrics and renders the Prometheus exposition.
type Registry struct {
	requestsTotal          atomic.Uint64
	requestsFailedTotal    atomic.Uint64
	redundantRequestsTotal atomic.Uint64
	latencySavedSeconds    atomic.Uint64 // float64 bits via math.Float64bits

	firstTokenLatency *histogram

	winsMu sync.Mutex
	wins   map[string]*atomic.Uint64

	lossesMu sync.Mutex
	losses   map[lossKey]*atomic.Uint64

	buildInfoMu sync.Mutex
	buildInfo   map[string]string // labels for hedge_build_info, nil until set

	inFlightFn func() int
	coolingFn  func() map[string]int
}

// NewRegistry creates a metrics registry. buckets sets the first-token latency
// histogram bounds (DefaultFirstTokenBuckets if nil).
func NewRegistry(buckets []float64) *Registry {
	if buckets == nil {
		buckets = DefaultFirstTokenBuckets
	}
	return &Registry{
		firstTokenLatency: newHistogram(buckets),
		wins:              make(map[string]*atomic.Uint64),
		losses:            make(map[lossKey]*atomic.Uint64),
		inFlightFn:        func() int { return 0 },
	}
}

// SetInFlightFunc installs the single source of truth for the hedge_inflight
// gauge: a function returning the engine's current mutex-protected in-flight
// count. It is read at scrape time.
func (r *Registry) SetInFlightFunc(fn func() int) {
	if fn != nil {
		r.inFlightFn = fn
	}
}

// SetCoolingFunc installs the scrape-time source for hedge_backend_cooling
// (0/1 per backend). Unset → the metric is still typed but emits no series.
func (r *Registry) SetCoolingFunc(fn func() map[string]int) {
	if fn != nil {
		r.coolingFn = fn
	}
}

// SetBuildInfo records the labels for the hedge_build_info gauge, a constant
// "1" series identifying the running build (e.g. version="v1.2.3",
// go_version="go1.26.4"). The line is rendered on /metrics only after this is
// called; labels are emitted in sorted, quoted form.
func (r *Registry) SetBuildInfo(version, goVersion string) {
	r.buildInfoMu.Lock()
	r.buildInfo = map[string]string{
		"version":    version,
		"go_version": goVersion,
	}
	r.buildInfoMu.Unlock()
}

// IncRequests increments hedge_requests_total.
func (r *Registry) IncRequests() { r.requestsTotal.Add(1) }

// IncFailedRequests increments hedge_requests_failed_total (requests that
// produced no winner — every backend failed, so the client got an error).
func (r *Registry) IncFailedRequests() { r.requestsFailedTotal.Add(1) }

// AddRedundantRequests adds n to hedge_redundant_requests_total (the number of
// speculative backups started beyond the primary).
func (r *Registry) AddRedundantRequests(n int) {
	if n > 0 {
		r.redundantRequestsTotal.Add(uint64(n))
	}
}

// ObserveWin records that the named backend won a request.
//
// The lock guards only the map (locate-or-insert the per-backend counter); the
// counter itself is an *atomic.Uint64. We intentionally UNLOCK before the
// atomic Add so the held critical section stays minimal — the increment needs no
// lock, and the counter pointer, once obtained, stays valid because entries are
// never deleted. winsSnapshot reads each counter with the same atomic Load, so a
// concurrent increment after the unlock is always observed safely.
func (r *Registry) ObserveWin(backend string) {
	r.winsMu.Lock()
	c := r.wins[backend]
	if c == nil {
		c = new(atomic.Uint64)
		r.wins[backend] = c
	}
	r.winsMu.Unlock()
	c.Add(1)
}

// ObserveLoss records that the named backend lost a request for the given
// reason (one of the hedge.LossReason* values). It mirrors ObserveWin's
// locking: the lock guards only the map (locate-or-insert the per-series
// counter), and the *atomic.Uint64 is incremented after the unlock. Reasons are
// bounded to a fixed set, so the map stays at backends × reasons entries.
func (r *Registry) ObserveLoss(backend, reason string) {
	k := lossKey{backend: backend, reason: reason}
	r.lossesMu.Lock()
	c := r.losses[k]
	if c == nil {
		c = new(atomic.Uint64)
		r.losses[k] = c
	}
	r.lossesMu.Unlock()
	c.Add(1)
}

// ObserveFirstToken records a first-token latency sample (seconds-valued
// duration) in the histogram.
func (r *Registry) ObserveFirstToken(d time.Duration) {
	r.firstTokenLatency.observe(d.Seconds())
}

// AddLatencySaved adds an estimated latency-saved duration to
// hedge_latency_saved_seconds_total. Negative values are ignored.
func (r *Registry) AddLatencySaved(d time.Duration) {
	if d <= 0 {
		return
	}
	for {
		old := r.latencySavedSeconds.Load()
		nv := math.Float64bits(math.Float64frombits(old) + d.Seconds())
		if r.latencySavedSeconds.CompareAndSwap(old, nv) {
			return
		}
	}
}

// winsSnapshot returns a sorted, stable view of per-backend win counts.
func (r *Registry) winsSnapshot() []struct {
	backend string
	value   uint64
} {
	r.winsMu.Lock()
	names := make([]string, 0, len(r.wins))
	for k := range r.wins {
		names = append(names, k)
	}
	out := make([]struct {
		backend string
		value   uint64
	}, 0, len(r.wins))
	for _, n := range names {
		out = append(out, struct {
			backend string
			value   uint64
		}{n, r.wins[n].Load()})
	}
	r.winsMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].backend < out[j].backend })
	return out
}

// lossSample is one rendered per-backend loss series.
type lossSample struct {
	backend string
	reason  string
	value   uint64
}

// lossesSnapshot returns a stable view of per-backend loss counts, sorted by
// (backend, reason) so the exposition is deterministic.
func (r *Registry) lossesSnapshot() []lossSample {
	r.lossesMu.Lock()
	out := make([]lossSample, 0, len(r.losses))
	for k, c := range r.losses {
		out = append(out, lossSample{backend: k.backend, reason: k.reason, value: c.Load()})
	}
	r.lossesMu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].backend != out[j].backend {
			return out[i].backend < out[j].backend
		}
		return out[i].reason < out[j].reason
	})
	return out
}

// WriteTo renders the full Prometheus text exposition to w.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	var b strings.Builder

	r.writeBuildInfo(&b)

	writeCounter(&b, "hedge_requests_total",
		"Total chat-completion requests handled.", r.requestsTotal.Load())

	writeCounter(&b, "hedge_requests_failed_total",
		"Requests that produced no winner (every backend failed).",
		r.requestsFailedTotal.Load())

	// Per-backend win counter (a single HELP/TYPE header, one line per label).
	b.WriteString("# HELP hedge_backend_wins_total Requests won by each backend.\n")
	b.WriteString("# TYPE hedge_backend_wins_total counter\n")
	for _, w := range r.winsSnapshot() {
		b.WriteString("hedge_backend_wins_total{backend=")
		b.WriteString(quoteLabelValue(w.backend))
		b.WriteString("} ")
		b.WriteString(strconv.FormatUint(w.value, 10))
		b.WriteByte('\n')
	}

	// Per-backend loss counter, labelled by backend AND reason. Label keys are
	// emitted in sorted order (backend, then reason) and values are quoted.
	b.WriteString("# HELP hedge_backend_losses_total Requests lost by each backend, by reason.\n")
	b.WriteString("# TYPE hedge_backend_losses_total counter\n")
	for _, l := range r.lossesSnapshot() {
		b.WriteString("hedge_backend_losses_total{backend=")
		b.WriteString(quoteLabelValue(l.backend))
		b.WriteString(",reason=")
		b.WriteString(quoteLabelValue(l.reason))
		b.WriteString("} ")
		b.WriteString(strconv.FormatUint(l.value, 10))
		b.WriteByte('\n')
	}

	writeCounter(&b, "hedge_redundant_requests_total",
		"Speculative backup requests started beyond the primary.",
		r.redundantRequestsTotal.Load())

	writeFloatCounter(&b, "hedge_latency_saved_seconds_total",
		"Estimated cumulative first-token latency saved by hedging.",
		math.Float64frombits(r.latencySavedSeconds.Load()))

	r.writeHistogram(&b, "hedge_first_token_latency_seconds",
		"First-token latency distribution (seconds).")

	writeGauge(&b, "hedge_inflight",
		"Speculative backends currently in flight across all requests.",
		float64(r.inFlightFn()))

	r.writeCooling(&b)

	n, err := io.WriteString(w, b.String())
	return int64(n), err
}

func (r *Registry) writeCooling(b *strings.Builder) {
	b.WriteString("# HELP hedge_backend_cooling Whether each backend is in loss cooldown (1) or eligible (0).\n")
	b.WriteString("# TYPE hedge_backend_cooling gauge\n")
	if r.coolingFn == nil {
		return
	}
	m := r.coolingFn()
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		b.WriteString("hedge_backend_cooling{backend=")
		b.WriteString(quoteLabelValue(n))
		b.WriteString("} ")
		b.WriteString(strconv.Itoa(m[n]))
		b.WriteByte('\n')
	}
}

func (r *Registry) writeHistogram(b *strings.Builder, name, help string) {
	snap := r.firstTokenLatency.snapshot()
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s histogram\n", name)
	for i, bound := range snap.bounds {
		fmt.Fprintf(b, "%s_bucket{le=%s} %d\n",
			name, quoteLabelValue(formatFloat(bound)), snap.cumulative[i])
	}
	// The mandatory +Inf bucket equals the total count.
	fmt.Fprintf(b, "%s_bucket{le=\"+Inf\"} %d\n", name, snap.count)
	fmt.Fprintf(b, "%s_sum %s\n", name, formatFloat(snap.sum))
	fmt.Fprintf(b, "%s_count %d\n", name, snap.count)
}

// writeBuildInfo renders the hedge_build_info gauge if build labels have been
// set. It emits nothing when SetBuildInfo has not been called.
func (r *Registry) writeBuildInfo(b *strings.Builder) {
	r.buildInfoMu.Lock()
	labels := r.buildInfo
	r.buildInfoMu.Unlock()
	if len(labels) == 0 {
		return
	}
	b.WriteString("# HELP hedge_build_info Build metadata for the running binary (constant 1).\n")
	b.WriteString("# TYPE hedge_build_info gauge\n")
	b.WriteString("hedge_build_info")
	writeLabels(b, labels)
	b.WriteString(" 1\n")
}

// writeLabels renders a label set in sorted key order with quoted values, e.g.
// {go_version="go1.26.4",version="dev"}. An empty set renders nothing.
func writeLabels(b *strings.Builder, labels map[string]string) {
	if len(labels) == 0 {
		return
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(quoteLabelValue(labels[k]))
	}
	b.WriteByte('}')
}

func writeCounter(b *strings.Builder, name, help string, v uint64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	fmt.Fprintf(b, "%s %d\n", name, v)
}

func writeFloatCounter(b *strings.Builder, name, help string, v float64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	fmt.Fprintf(b, "%s %s\n", name, formatFloat(v))
}

func writeGauge(b *strings.Builder, name, help string, v float64) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, help)
	fmt.Fprintf(b, "# TYPE %s gauge\n", name)
	fmt.Fprintf(b, "%s %s\n", name, formatFloat(v))
}

// formatFloat renders a float in a Prometheus-friendly way (shortest round-trip
// representation, with explicit +Inf handling).
func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// quoteLabelValue returns a double-quoted, escaped Prometheus label value.
func quoteLabelValue(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
