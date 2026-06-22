// Package policy defines the hedging policy and the pure decision logic the
// hedge engine consults when deciding whether to start another speculative
// backend.
//
// The policy is deliberately a small value type with side-effect-free decision
// methods, so it is trivially testable in isolation across the full grid of
// elapsed-time / started-count / cost combinations.
package policy

import "time"

// HedgePolicy controls how aggressively the engine fires speculative duplicate
// requests.
type HedgePolicy struct {
	// FireAfter is how long to wait, after the most recent backend start,
	// before starting the next backup if no usable token has arrived yet.
	FireAfter time.Duration

	// MaxInFlight is the maximum number of concurrently-running backends for a
	// single request (the primary counts as one). A value <= 0 is treated as 1
	// (no hedging).
	MaxInFlight int

	// CostCeiling bounds speculative request STARTS, not token spend: the
	// engine will not start a backend if doing so would push the cumulative
	// per-request start cost (sum of each started backend's CostPerRequest)
	// above this ceiling. A value <= 0 disables the cost gate (only
	// MaxInFlight applies). See the README for the honest semantics: this caps
	// how many duplicate requests may be launched, not the upstream token bill.
	CostCeiling float64
}

// DefaultPolicy returns a conservative starting policy: hedge a single backup
// after 250ms, at most two backends in flight, no cost gate.
func DefaultPolicy() HedgePolicy {
	return HedgePolicy{
		FireAfter:   250 * time.Millisecond,
		MaxInFlight: 2,
		CostCeiling: 0,
	}
}

// effectiveMaxInFlight normalises a non-positive MaxInFlight to 1.
func (p HedgePolicy) effectiveMaxInFlight() int {
	if p.MaxInFlight <= 0 {
		return 1
	}
	return p.MaxInFlight
}

// AllowStart reports whether the engine may start one more backend, given how
// many are already in flight, the cumulative speculative cost already
// committed, and the cost of the candidate backend about to be started.
//
// It enforces both bounds atomically from the engine's perspective: the engine
// calls this under its in-flight mutex as part of a check-and-increment, so two
// goroutines can never both observe headroom and both start.
//
//   - inFlight: number of backends currently started for this request.
//   - committedCost: sum of CostPerRequest for already-started backends.
//   - candidateCost: CostPerRequest of the backend being considered.
func (p HedgePolicy) AllowStart(inFlight int, committedCost, candidateCost float64) bool {
	if inFlight >= p.effectiveMaxInFlight() {
		return false
	}
	if p.CostCeiling > 0 && committedCost+candidateCost > p.CostCeiling {
		return false
	}
	return true
}

// HasHeadroom reports whether, ignoring cost, more than the current number of
// backends are permitted. It lets the engine cheaply decide whether arming a
// fire-after timer is even worthwhile.
func (p HedgePolicy) HasHeadroom(inFlight int) bool {
	return inFlight < p.effectiveMaxInFlight()
}
