package policy

import (
	"testing"
	"time"
)

func TestDefaultPolicy(t *testing.T) {
	p := DefaultPolicy()
	if p.FireAfter != 250*time.Millisecond {
		t.Errorf("FireAfter=%v", p.FireAfter)
	}
	if p.MaxInFlight != 2 {
		t.Errorf("MaxInFlight=%d", p.MaxInFlight)
	}
}

func TestAllowStartMaxInFlight(t *testing.T) {
	p := HedgePolicy{MaxInFlight: 2}
	tests := []struct {
		inFlight int
		want     bool
	}{
		{0, true},  // primary slot
		{1, true},  // one backup allowed
		{2, false}, // at cap
		{3, false}, // over cap
	}
	for _, tc := range tests {
		if got := p.AllowStart(tc.inFlight, 0, 1); got != tc.want {
			t.Errorf("AllowStart(inFlight=%d)=%v want %v", tc.inFlight, got, tc.want)
		}
	}
}

func TestAllowStartZeroMaxInFlightTreatedAsOne(t *testing.T) {
	p := HedgePolicy{MaxInFlight: 0}
	if !p.AllowStart(0, 0, 1) {
		t.Error("primary should start with MaxInFlight=0")
	}
	if p.AllowStart(1, 0, 1) {
		t.Error("no backup should start with effective MaxInFlight=1")
	}
}

func TestAllowStartCostCeiling(t *testing.T) {
	// Ceiling 3.0; each backend costs 1.0. With 2 already committed (cost 2.0),
	// adding one more (→3.0) is allowed (not over). A fourth (→4.0) is blocked.
	p := HedgePolicy{MaxInFlight: 10, CostCeiling: 3.0}
	if !p.AllowStart(2, 2.0, 1.0) {
		t.Error("start to exactly the ceiling should be allowed")
	}
	if p.AllowStart(3, 3.0, 1.0) {
		t.Error("start exceeding the ceiling should be blocked")
	}
}

func TestAllowStartCostCeilingDisabledWhenZero(t *testing.T) {
	p := HedgePolicy{MaxInFlight: 10, CostCeiling: 0}
	if !p.AllowStart(5, 1000, 1000) {
		t.Error("cost gate should be disabled when CostCeiling<=0")
	}
}

func TestAllowStartExpensiveCandidateBlocked(t *testing.T) {
	p := HedgePolicy{MaxInFlight: 10, CostCeiling: 5.0}
	// committed 4.0, candidate 2.0 → 6.0 > 5.0 blocked.
	if p.AllowStart(1, 4.0, 2.0) {
		t.Error("expensive candidate exceeding ceiling should be blocked")
	}
	// committed 4.0, candidate 1.0 → 5.0 allowed.
	if !p.AllowStart(1, 4.0, 1.0) {
		t.Error("candidate landing exactly on ceiling should be allowed")
	}
}

func TestHasHeadroom(t *testing.T) {
	p := HedgePolicy{MaxInFlight: 3}
	if !p.HasHeadroom(2) {
		t.Error("headroom should exist below cap")
	}
	if p.HasHeadroom(3) {
		t.Error("no headroom at cap")
	}
}
