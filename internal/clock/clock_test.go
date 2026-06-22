package clock

import (
	"testing"
	"time"
)

func TestFakeClockAfterFiresOnAdvance(t *testing.T) {
	c := NewFakeClock(time.Time{})
	ch := c.After(100 * time.Millisecond)

	select {
	case <-ch:
		t.Fatal("timer fired before advance")
	default:
	}

	c.Advance(50 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("timer fired before its deadline")
	default:
	}

	c.Advance(50 * time.Millisecond)
	select {
	case <-ch:
		// expected
	default:
		t.Fatal("timer did not fire after reaching deadline")
	}
}

func TestFakeClockNowAdvances(t *testing.T) {
	start := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	c := NewFakeClock(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Now=%v want %v", c.Now(), start)
	}
	c.Advance(2 * time.Second)
	if got := c.Now(); !got.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("Now=%v want %v", got, start.Add(2*time.Second))
	}
}

func TestFakeClockNonPositiveFiresImmediately(t *testing.T) {
	c := NewFakeClock(time.Time{})
	ch := c.After(0)
	select {
	case <-ch:
	default:
		t.Fatal("zero-duration timer should fire immediately")
	}
}

func TestFakeClockMultipleTimersOrdered(t *testing.T) {
	c := NewFakeClock(time.Time{})
	a := c.After(30 * time.Millisecond)
	b := c.After(10 * time.Millisecond)

	c.Advance(10 * time.Millisecond)
	select {
	case <-b:
	default:
		t.Fatal("earlier timer b should have fired")
	}
	select {
	case <-a:
		t.Fatal("later timer a should not have fired yet")
	default:
	}

	c.Advance(20 * time.Millisecond)
	select {
	case <-a:
	default:
		t.Fatal("later timer a should have fired")
	}
}

func TestFakeClockSetMovesForwardOnly(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewFakeClock(start)
	c.Set(start.Add(-time.Hour)) // backwards: ignored
	if !c.Now().Equal(start) {
		t.Fatalf("Set moved clock backwards: %v", c.Now())
	}
	c.Set(start.Add(time.Hour))
	if !c.Now().Equal(start.Add(time.Hour)) {
		t.Fatalf("Set did not advance: %v", c.Now())
	}
}

func TestRealClockBasic(t *testing.T) {
	var c RealClock
	before := time.Now()
	got := c.Now()
	if got.Before(before) {
		t.Fatal("RealClock.Now went backwards")
	}
	select {
	case <-c.After(time.Millisecond):
	case <-time.After(time.Second):
		t.Fatal("RealClock.After never fired")
	}
}

func TestFakeClockTimerFiresOnAdvance(t *testing.T) {
	c := NewFakeClock(time.Time{})
	tm := c.NewTimer(100 * time.Millisecond)

	c.Advance(50 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatal("timer fired before its deadline")
	default:
	}

	c.Advance(50 * time.Millisecond)
	select {
	case <-tm.C():
		// expected
	default:
		t.Fatal("timer did not fire after reaching deadline")
	}
}

// Reset re-arms the SAME timer (single channel) for a new deadline and must not
// deliver a stale tick from before the reset.
func TestFakeClockTimerResetReArms(t *testing.T) {
	c := NewFakeClock(time.Time{})
	tm := c.NewTimer(10 * time.Millisecond)

	// Re-arm before it fires; the original 10ms deadline must be discarded.
	tm.Reset(100 * time.Millisecond)

	c.Advance(50 * time.Millisecond) // past the OLD deadline, before the new one
	select {
	case <-tm.C():
		t.Fatal("timer fired at the discarded old deadline after Reset")
	default:
	}

	c.Advance(60 * time.Millisecond) // past the new 100ms deadline
	select {
	case <-tm.C():
		// expected: fires once for the new deadline
	default:
		t.Fatal("timer did not fire at the new deadline after Reset")
	}
}

// A Reset issued AFTER a fire (with the tick still pending in the channel) must
// drain that stale tick, so the re-armed timer fires only for the new deadline.
func TestFakeClockTimerResetDrainsStaleTick(t *testing.T) {
	c := NewFakeClock(time.Time{})
	tm := c.NewTimer(10 * time.Millisecond)

	c.Advance(10 * time.Millisecond) // fires; tick now buffered, undrained
	tm.Reset(10 * time.Millisecond)  // must drain the stale tick

	select {
	case <-tm.C():
		t.Fatal("stale tick was not drained by Reset")
	default:
	}

	c.Advance(10 * time.Millisecond)
	select {
	case <-tm.C():
		// expected
	default:
		t.Fatal("re-armed timer did not fire for the new deadline")
	}
}

func TestFakeClockTimerStopPreventsFire(t *testing.T) {
	c := NewFakeClock(time.Time{})
	tm := c.NewTimer(10 * time.Millisecond)
	tm.Stop()
	tm.Stop() // idempotent

	c.Advance(50 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatal("stopped timer fired")
	default:
	}
}

func TestRealClockTimer(t *testing.T) {
	var c RealClock
	tm := c.NewTimer(time.Millisecond)
	select {
	case <-tm.C():
	case <-time.After(time.Second):
		t.Fatal("RealClock timer never fired")
	}
	// Re-arm and fire again.
	tm.Reset(time.Millisecond)
	select {
	case <-tm.C():
	case <-time.After(time.Second):
		t.Fatal("RealClock timer did not fire after Reset")
	}
	// Stop after fire is safe.
	tm.Stop()
	// A fresh timer that is stopped before firing should not fire.
	tm2 := c.NewTimer(time.Hour)
	tm2.Stop()
	select {
	case <-tm2.C():
		t.Fatal("stopped RealClock timer fired")
	case <-time.After(20 * time.Millisecond):
		// expected: no fire
	}
}
