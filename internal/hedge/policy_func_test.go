package hedge

import (
	"context"
	"testing"
	"time"

	"hedge-llm/internal/backend"
	"hedge-llm/internal/clock"
	"hedge-llm/internal/oapi"
	"hedge-llm/internal/policy"
)

// reqFor builds a request naming a specific model, so a per-model policy
// resolver has something to key on.
func reqFor(model string) *oapi.Request {
	return &oapi.Request{Model: model, Messages: []oapi.Message{{Role: "user", Content: "hi"}}}
}

// startedForModel races the engine for one model until the cap settles, then
// cancels and reports how many backends the run actually started. No backend
// ever produces a token, so the only thing bounding starts is the policy.
func startedForModel(t *testing.T, e *Engine, model string) int {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan int, 1)
	go func() {
		o, _ := e.Run(ctx, reqFor(model), &captureSink{})
		ch <- o.Started
	}()
	// Real time for the fake clock to drive several fire-after windows.
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case n := <-ch:
		return n
	case <-time.After(10 * time.Second):
		t.Fatal("engine.Run did not return (possible deadlock)")
		return 0
	}
}

func stalledBackends(clk *clock.FakeClock, names ...string) []backend.Backend {
	out := make([]backend.Backend, 0, len(names))
	for _, n := range names {
		out = append(out, &backend.FakeBackend{
			BackendName:     n,
			Clock:           clk,
			FirstTokenDelay: time.Hour, // nobody wins; only the policy limits starts
			Tokens:          []string{"x"},
		})
	}
	return out
}

// Two models served by one engine get different in-flight caps.
func TestPolicyFuncAppliesPerModelCap(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	base := policy.HedgePolicy{FireAfter: 3 * time.Millisecond, MaxInFlight: 2}
	perModel := map[string]policy.HedgePolicy{
		"cheap":     {FireAfter: 3 * time.Millisecond, MaxInFlight: 4},
		"expensive": {FireAfter: 3 * time.Millisecond, MaxInFlight: 1},
	}
	e := NewEngine(stalledBackends(clk, "a", "b", "c", "d"), base, clk,
		WithPolicyFunc(func(model string) policy.HedgePolicy {
			if p, ok := perModel[model]; ok {
				return p
			}
			return base
		}))

	d := startDriver(clk, 2*time.Millisecond)
	defer d.Stop()

	if got := startedForModel(t, e, "expensive"); got != 1 {
		t.Errorf(`model "expensive": started=%d, want 1 (max_in_flight=1)`, got)
	}
	if got := startedForModel(t, e, "cheap"); got != 4 {
		t.Errorf(`model "cheap": started=%d, want 4 (max_in_flight=4)`, got)
	}
}

// A model the resolver does not know falls back to the engine's own policy.
func TestPolicyFuncUnknownModelUsesEngineDefault(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	base := policy.HedgePolicy{FireAfter: 3 * time.Millisecond, MaxInFlight: 2}
	e := NewEngine(stalledBackends(clk, "a", "b", "c", "d"), base, clk,
		WithPolicyFunc(func(model string) policy.HedgePolicy {
			if model == "known" {
				return policy.HedgePolicy{FireAfter: 3 * time.Millisecond, MaxInFlight: 4}
			}
			return base
		}))

	d := startDriver(clk, 2*time.Millisecond)
	defer d.Stop()

	if got := startedForModel(t, e, "something-else"); got != 2 {
		t.Errorf("unknown model: started=%d, want 2 (engine default)", got)
	}
}

// With no resolver installed every model gets the single configured policy.
func TestNoPolicyFuncKeepsSinglePolicy(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	base := policy.HedgePolicy{FireAfter: 3 * time.Millisecond, MaxInFlight: 3}
	e := NewEngine(stalledBackends(clk, "a", "b", "c", "d"), base, clk)

	d := startDriver(clk, 2*time.Millisecond)
	defer d.Stop()

	for _, model := range []string{"one", "two"} {
		if got := startedForModel(t, e, model); got != 3 {
			t.Errorf("model %q: started=%d, want 3", model, got)
		}
	}
}

// A per-model cost_ceiling bounds starts independently of max_in_flight.
func TestPolicyFuncAppliesPerModelCostCeiling(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	mk := func(name string) backend.Backend {
		return &backend.FakeBackend{
			BackendName: name, Clock: clk, Cost: 1,
			FirstTokenDelay: time.Hour,
			Tokens:          []string{"x"},
		}
	}
	base := policy.HedgePolicy{FireAfter: 3 * time.Millisecond, MaxInFlight: 10}
	e := NewEngine([]backend.Backend{mk("a"), mk("b"), mk("c"), mk("d")}, base, clk,
		WithPolicyFunc(func(model string) policy.HedgePolicy {
			if model == "budgeted" {
				p := base
				p.CostCeiling = 2.0
				return p
			}
			return base
		}))

	d := startDriver(clk, 2*time.Millisecond)
	defer d.Stop()

	if got := startedForModel(t, e, "budgeted"); got != 2 {
		t.Errorf("budgeted model: started=%d, want 2 (cost_ceiling=2.0 at 1.0 each)", got)
	}
}

// The resolver's fire_after is what the race actually waits on: a long
// per-model delay keeps the backup unstarted for the whole observation window,
// while a short one lets it through.
func TestPolicyFuncAppliesPerModelFireAfter(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fireAfter time.Duration
		want      int
	}{
		{"slow model holds the backup back", time.Hour, 1},
		{"fast model releases the backup", 3 * time.Millisecond, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewFakeClock(time.Time{})
			base := policy.HedgePolicy{FireAfter: time.Hour, MaxInFlight: 2}
			fireAfter := tc.fireAfter
			e := NewEngine(stalledBackends(clk, "a", "b"), base, clk,
				WithPolicyFunc(func(string) policy.HedgePolicy {
					return policy.HedgePolicy{FireAfter: fireAfter, MaxInFlight: 2}
				}))
			d := startDriver(clk, 2*time.Millisecond)
			defer d.Stop()
			if got := startedForModel(t, e, "m"); got != tc.want {
				t.Errorf("started=%d, want %d", got, tc.want)
			}
		})
	}
}
