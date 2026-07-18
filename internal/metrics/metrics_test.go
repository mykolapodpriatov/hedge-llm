package metrics

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// parsedExposition is a lightweight parse of Prometheus text used to validate
// structure (not just non-emptiness).
type parsedExposition struct {
	help    map[string]string
	typ     map[string]string
	samples []sample
}

type sample struct {
	name   string
	labels map[string]string
	value  string
}

func parseExposition(t *testing.T, text string) parsedExposition {
	t.Helper()
	p := parsedExposition{help: map[string]string{}, typ: map[string]string{}}
	for _, line := range strings.Split(text, "\n") {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "# HELP ") {
			rest := strings.TrimPrefix(line, "# HELP ")
			parts := strings.SplitN(rest, " ", 2)
			p.help[parts[0]] = ""
			if len(parts) == 2 {
				p.help[parts[0]] = parts[1]
			}
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			rest := strings.TrimPrefix(line, "# TYPE ")
			parts := strings.SplitN(rest, " ", 2)
			if len(parts) != 2 {
				t.Fatalf("malformed TYPE line: %q", line)
			}
			p.typ[parts[0]] = parts[1]
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// metric{labels} value  OR  metric value
		sp := strings.LastIndex(line, " ")
		if sp < 0 {
			t.Fatalf("malformed sample line: %q", line)
		}
		lhs, val := line[:sp], line[sp+1:]
		s := sample{value: val, labels: map[string]string{}}
		if brace := strings.IndexByte(lhs, '{'); brace >= 0 {
			s.name = lhs[:brace]
			labelStr := strings.TrimSuffix(lhs[brace+1:], "}")
			s.labels = parseLabels(t, labelStr)
		} else {
			s.name = lhs
		}
		p.samples = append(p.samples, s)
	}
	return p
}

// parseLabels handles label sets including quoted values with the values we
// emit (le="+Inf", backend="name").
func parseLabels(t *testing.T, s string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if s == "" {
		return out
	}
	// Our exposition emits at most one label per series, which keeps this simple.
	for _, kv := range splitTopLevelCommas(s) {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			t.Fatalf("malformed label: %q", kv)
		}
		key := kv[:eq]
		val := kv[eq+1:]
		val = strings.TrimPrefix(val, `"`)
		val = strings.TrimSuffix(val, `"`)
		out[key] = val
	}
	return out
}

func splitTopLevelCommas(s string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for _, r := range s {
		switch r {
		case '"':
			inQuote = !inQuote
			cur.WriteRune(r)
		case ',':
			if inQuote {
				cur.WriteRune(r)
			} else {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var b strings.Builder
	if _, err := r.WriteTo(&b); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return b.String()
}

func TestExpositionTypesAndCounters(t *testing.T) {
	r := NewRegistry(nil)
	r.IncRequests()
	r.IncRequests()
	r.AddRedundantRequests(3)
	r.SetInFlightFunc(func() int { return 2 })

	p := parseExposition(t, render(t, r))

	if p.typ["hedge_requests_total"] != "counter" {
		t.Errorf("requests type=%q", p.typ["hedge_requests_total"])
	}
	if p.typ["hedge_inflight"] != "gauge" {
		t.Errorf("inflight type=%q", p.typ["hedge_inflight"])
	}
	if p.typ["hedge_first_token_latency_seconds"] != "histogram" {
		t.Errorf("histogram type=%q", p.typ["hedge_first_token_latency_seconds"])
	}

	if got := sampleValue(p, "hedge_requests_total", nil); got != "2" {
		t.Errorf("requests_total=%s want 2", got)
	}
	if got := sampleValue(p, "hedge_redundant_requests_total", nil); got != "3" {
		t.Errorf("redundant=%s want 3", got)
	}
	if got := sampleValue(p, "hedge_inflight", nil); got != "2" {
		t.Errorf("inflight=%s want 2", got)
	}

	// Every metric must have HELP and TYPE.
	for _, name := range []string{
		"hedge_requests_total", "hedge_requests_failed_total",
		"hedge_backend_wins_total", "hedge_backend_losses_total",
		"hedge_redundant_requests_total", "hedge_latency_saved_seconds_total",
		"hedge_first_token_latency_seconds", "hedge_inflight",
	} {
		if _, ok := p.help[name]; !ok {
			t.Errorf("missing HELP for %s", name)
		}
		if _, ok := p.typ[name]; !ok {
			t.Errorf("missing TYPE for %s", name)
		}
	}
}

func TestBackendWinsSortedAndQuoted(t *testing.T) {
	r := NewRegistry(nil)
	r.ObserveWin("zebra")
	r.ObserveWin("alpha")
	r.ObserveWin("alpha")
	r.ObserveWin(`weird"name`)

	text := render(t, r)
	p := parseExposition(t, text)

	// Collect wins series in emission order and check backend labels are sorted.
	var order []string
	for _, s := range p.samples {
		if s.name == "hedge_backend_wins_total" {
			order = append(order, s.labels["backend"])
		}
	}
	if len(order) != 3 {
		t.Fatalf("want 3 win series, got %d (%v)", len(order), order)
	}
	for i := 1; i < len(order); i++ {
		if order[i-1] > order[i] {
			t.Errorf("backend labels not sorted: %v", order)
		}
	}
	if got := sampleValue(p, "hedge_backend_wins_total", map[string]string{"backend": "alpha"}); got != "2" {
		t.Errorf("alpha wins=%s want 2", got)
	}
	// Quoting: the weird name's quote must be escaped in the raw text.
	if !strings.Contains(text, `backend="weird\"name"`) {
		t.Errorf("label value not properly quoted/escaped:\n%s", text)
	}
}

func TestBackendLossesSortedQuotedAndBounded(t *testing.T) {
	r := NewRegistry(nil)
	// Two backends across the fixed reason set, plus a weird name to test quoting.
	r.ObserveLoss("beta", "canceled")
	r.ObserveLoss("beta", "error")
	r.ObserveLoss("alpha", "no_usable_token")
	r.ObserveLoss("alpha", "no_usable_token") // same series → value 2 (cardinality bound)
	r.ObserveLoss("alpha", "error")
	r.ObserveLoss(`weird"name`, "canceled")

	text := render(t, r)
	p := parseExposition(t, text)

	type br struct{ backend, reason string }
	var order []br
	for _, s := range p.samples {
		if s.name == "hedge_backend_losses_total" {
			order = append(order, br{s.labels["backend"], s.labels["reason"]})
		}
	}
	if len(order) == 0 {
		t.Fatal("no loss series emitted")
	}
	// Series must be sorted by (backend, then reason).
	for i := 1; i < len(order); i++ {
		a, b := order[i-1], order[i]
		if a.backend > b.backend || (a.backend == b.backend && a.reason > b.reason) {
			t.Errorf("loss series not sorted by (backend,reason): %v", order)
		}
	}
	// A repeated observation accumulates on ONE series (cardinality is bounded to
	// distinct backend×reason pairs, never one series per event).
	if got := sampleValue(p, "hedge_backend_losses_total", map[string]string{"backend": "alpha", "reason": "no_usable_token"}); got != "2" {
		t.Errorf("alpha/no_usable_token=%s want 2", got)
	}
	seen := map[br]bool{}
	for _, o := range order {
		seen[o] = true
	}
	if len(order) != len(seen) {
		t.Errorf("duplicate series emitted (cardinality unbounded?): %v", order)
	}
	if len(seen) != 5 {
		t.Errorf("distinct loss series=%d want 5 (backends × reasons observed)", len(seen))
	}
	// Quoting: the weird name's quote must be escaped in the raw text, with the
	// reason label present alongside it.
	if !strings.Contains(text, `backend="weird\"name",reason="canceled"`) {
		t.Errorf("label value not properly quoted/escaped:\n%s", text)
	}
	if p.typ["hedge_backend_losses_total"] != "counter" {
		t.Errorf("losses type=%q", p.typ["hedge_backend_losses_total"])
	}
}

func TestRequestsFailedCounter(t *testing.T) {
	r := NewRegistry(nil)
	r.IncFailedRequests()
	r.IncFailedRequests()
	p := parseExposition(t, render(t, r))
	if p.typ["hedge_requests_failed_total"] != "counter" {
		t.Errorf("requests_failed type=%q", p.typ["hedge_requests_failed_total"])
	}
	if got := sampleValue(p, "hedge_requests_failed_total", nil); got != "2" {
		t.Errorf("requests_failed=%s want 2", got)
	}
}

func TestHistogramCumulativeAndInfBucket(t *testing.T) {
	r := NewRegistry([]float64{0.1, 0.5, 1.0})
	// Observations (seconds): 0.05, 0.2, 0.2, 0.7, 5.0
	for _, d := range []time.Duration{
		50 * time.Millisecond,
		200 * time.Millisecond,
		200 * time.Millisecond,
		700 * time.Millisecond,
		5 * time.Second,
	} {
		r.ObserveFirstToken(d)
	}
	p := parseExposition(t, render(t, r))

	name := "hedge_first_token_latency_seconds"
	// Cumulative bucket counts:
	// le=0.1 → {0.05} = 1
	// le=0.5 → {0.05,0.2,0.2} = 3
	// le=1.0 → {..,0.7} = 4
	// le=+Inf → all 5
	checkBucket(t, p, name, "0.1", 1)
	checkBucket(t, p, name, "0.5", 3)
	checkBucket(t, p, name, "1", 4)
	checkBucket(t, p, name, "+Inf", 5)

	// _count equals +Inf bucket; _sum is the total seconds.
	if got := sampleValue(p, name+"_count", nil); got != "5" {
		t.Errorf("_count=%s want 5", got)
	}
	wantSum := 0.05 + 0.2 + 0.2 + 0.7 + 5.0
	gotSum, err := strconv.ParseFloat(sampleValue(p, name+"_sum", nil), 64)
	if err != nil {
		t.Fatalf("parse _sum: %v", err)
	}
	if diff := gotSum - wantSum; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("_sum=%v want %v", gotSum, wantSum)
	}

	// Buckets must be monotonically non-decreasing (cumulative invariant).
	var prev int64 = -1
	for _, s := range p.samples {
		if s.name == name+"_bucket" {
			v, _ := strconv.ParseInt(s.value, 10, 64)
			if v < prev {
				t.Errorf("buckets not cumulative: %d after %d", v, prev)
			}
			prev = v
		}
	}
}

func TestLatencySavedFloatCounter(t *testing.T) {
	r := NewRegistry(nil)
	r.AddLatencySaved(150 * time.Millisecond)
	r.AddLatencySaved(350 * time.Millisecond)
	r.AddLatencySaved(-time.Second) // ignored
	p := parseExposition(t, render(t, r))
	got, err := strconv.ParseFloat(sampleValue(p, "hedge_latency_saved_seconds_total", nil), 64)
	if err != nil {
		t.Fatal(err)
	}
	if diff := got - 0.5; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("latency_saved=%v want 0.5", got)
	}
}

func TestInFlightGaugeReadsLive(t *testing.T) {
	r := NewRegistry(nil)
	live := 0
	r.SetInFlightFunc(func() int { return live })
	live = 5
	p := parseExposition(t, render(t, r))
	if got := sampleValue(p, "hedge_inflight", nil); got != "5" {
		t.Errorf("inflight=%s want live 5", got)
	}
}

// --- helpers ----------------------------------------------------------------

func sampleValue(p parsedExposition, name string, labels map[string]string) string {
	for _, s := range p.samples {
		if s.name != name {
			continue
		}
		if labelsMatch(s.labels, labels) {
			return s.value
		}
	}
	return ""
}

func labelsMatch(got, want map[string]string) bool {
	if len(want) == 0 {
		return len(got) == 0
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func checkBucket(t *testing.T, p parsedExposition, name, le string, want int64) {
	t.Helper()
	v := sampleValue(p, name+"_bucket", map[string]string{"le": le})
	if v == "" {
		t.Fatalf("missing bucket le=%s", le)
	}
	got, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("bucket le=%s value %q: %v", le, v, err)
	}
	if got != want {
		t.Errorf("bucket le=%s = %d want %d", le, got, want)
	}
}
