package adaptive

import (
	"sync"
	"testing"
	"time"
)

func TestPercentileEmpty(t *testing.T) {
	e := NewEstimator(8)
	if _, ok := e.Percentile("a", 0.5); ok {
		t.Error("expected ok=false for no samples")
	}
}

func TestPercentileBasic(t *testing.T) {
	e := NewEstimator(16)
	for i := 1; i <= 10; i++ {
		e.Observe("a", time.Duration(i)*time.Millisecond)
	}
	p50, ok := e.Percentile("a", 0.5)
	if !ok {
		t.Fatal("expected samples")
	}
	// Nearest-rank p50 over 1..10ms → rank int(0.5*9+0.5)=5 → samples[5]=6ms.
	if p50 != 6*time.Millisecond {
		t.Errorf("p50=%v want 6ms", p50)
	}
	p100, _ := e.Percentile("a", 1.0)
	if p100 != 10*time.Millisecond {
		t.Errorf("p100=%v want 10ms", p100)
	}
	p0, _ := e.Percentile("a", 0.0)
	if p0 != 1*time.Millisecond {
		t.Errorf("p0=%v want 1ms", p0)
	}
}

func TestRingBufferBoundedMemory(t *testing.T) {
	e := NewEstimator(4)
	// Add far more than the window; only the last 4 should survive.
	for i := 1; i <= 100; i++ {
		e.Observe("a", time.Duration(i)*time.Millisecond)
	}
	e.mu.Lock()
	r := e.rings["a"]
	count := r.count
	bufLen := len(r.buf)
	e.mu.Unlock()
	if count != 4 || bufLen != 4 {
		t.Fatalf("ring not bounded: count=%d bufLen=%d", count, bufLen)
	}
	// Max should be 100ms (last value), min 97ms (window = 97..100).
	pMax, _ := e.Percentile("a", 1.0)
	if pMax != 100*time.Millisecond {
		t.Errorf("max=%v want 100ms", pMax)
	}
	pMin, _ := e.Percentile("a", 0.0)
	if pMin != 97*time.Millisecond {
		t.Errorf("min=%v want 97ms", pMin)
	}
}

func TestSuggestFireAfterFallsBackBelowMinSamples(t *testing.T) {
	e := NewEstimator(16)
	def := 200 * time.Millisecond
	e.Observe("p", 10*time.Millisecond)
	e.Observe("p", 12*time.Millisecond)
	// minSamples=5, only 2 → fallback.
	if got := e.SuggestFireAfter("p", def, 5); got != def {
		t.Errorf("SuggestFireAfter=%v want default %v", got, def)
	}
}

func TestSuggestFireAfterUsesP50(t *testing.T) {
	e := NewEstimator(16)
	def := 200 * time.Millisecond
	for i := 1; i <= 10; i++ {
		e.Observe("p", time.Duration(i)*time.Millisecond)
	}
	got := e.SuggestFireAfter("p", def, 5)
	if got != 6*time.Millisecond {
		t.Errorf("SuggestFireAfter=%v want p50 6ms", got)
	}
}

func TestEstimatorConcurrentUpdates(t *testing.T) {
	// Exercised under -race: many goroutines observe while others read.
	e := NewEstimator(64)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				e.Observe("backend", time.Duration(i)*time.Microsecond)
				_, _ = e.Percentile("backend", 0.5)
				_ = e.SuggestFireAfter("backend", time.Millisecond, 1)
			}
		}(g)
	}
	wg.Wait()
}

func TestDefaultWindowApplied(t *testing.T) {
	e := NewEstimator(0)
	if e.window != DefaultWindow {
		t.Errorf("window=%d want %d", e.window, DefaultWindow)
	}
}
