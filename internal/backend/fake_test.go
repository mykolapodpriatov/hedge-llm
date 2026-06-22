package backend

import (
	"context"
	"errors"
	"testing"
	"time"

	"hedge-llm/internal/clock"
	"hedge-llm/internal/oapi"
)

// drain collects all chunks from a channel until it closes (with a real-time
// safety timeout so a buggy producer can't hang the test).
func drain(t *testing.T, ch <-chan oapi.Chunk) []oapi.Chunk {
	t.Helper()
	var out []oapi.Chunk
	timeout := time.After(2 * time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, c)
		case <-timeout:
			t.Fatal("timed out draining channel (producer did not close)")
			return out
		}
	}
}

func TestFakeBackendStartError(t *testing.T) {
	fb := &FakeBackend{BackendName: "x", Clock: clock.NewFakeClock(time.Time{}), StartErr: errors.New("boom")}
	_, err := fb.Stream(context.Background(), &oapi.Request{})
	if err == nil {
		t.Fatal("expected start error")
	}
}

func TestFakeBackendEmitsTokensWithClock(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	fb := &FakeBackend{
		BackendName:     "fast",
		Clock:           clk,
		FirstTokenDelay: 10 * time.Millisecond,
		InterTokenDelay: 5 * time.Millisecond,
		Tokens:          []string{"a", "b", "c"},
		EmitFinish:      true,
	}
	ctx := context.Background()
	ch, err := fb.Stream(ctx, &oapi.Request{})
	if err != nil {
		t.Fatal(err)
	}

	// No token yet (first-token delay pending).
	select {
	case c := <-ch:
		t.Fatalf("got chunk before first-token delay: %+v", c)
	case <-time.After(20 * time.Millisecond):
	}

	// Advance enough to release first token + all inter-token delays + finish.
	// We advance generously; the FakeClock fires each pending one-shot timer as
	// the producer re-arms it. Advance in steps to let the producer progress.
	go func() {
		for i := 0; i < 6; i++ {
			time.Sleep(2 * time.Millisecond)
			clk.Advance(10 * time.Millisecond)
		}
	}()

	got := drain(t, ch)
	// 3 content tokens + 1 finish chunk.
	if len(got) != 4 {
		t.Fatalf("got %d chunks want 4: %+v", len(got), got)
	}
	contents := []string{}
	for _, c := range got {
		if u := c.UsableContent(); u != "" {
			contents = append(contents, u)
		}
	}
	if len(contents) != 3 || contents[0] != "a" || contents[2] != "c" {
		t.Errorf("contents=%v", contents)
	}
	// Each chunk should carry a Raw payload for relaying.
	if len(got[0].Raw) == 0 {
		t.Error("chunk Raw should be populated")
	}
}

func TestFakeBackendHonorsCancel(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	fb := &FakeBackend{
		BackendName:     "slow",
		Clock:           clk,
		FirstTokenDelay: time.Hour, // never reached
		Tokens:          []string{"x"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := fb.Stream(ctx, &oapi.Request{})
	if err != nil {
		t.Fatal(err)
	}
	// Cancel before the (huge) first-token delay elapses.
	cancel()
	// Channel must close promptly without emitting anything.
	got := drain(t, ch)
	if len(got) != 0 {
		t.Errorf("expected no chunks after cancel, got %d", len(got))
	}
}

func TestFakeBackendFailAfterIsLoss(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	fb := &FakeBackend{
		BackendName: "truncated",
		Clock:       clk,
		Tokens:      []string{"a", "b", "c"},
		FailAfter:   1, // emit only "a" then close (truncated)
	}
	ch, err := fb.Stream(context.Background(), &oapi.Request{})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch)
	if len(got) != 1 || got[0].UsableContent() != "a" {
		t.Errorf("got=%+v want only 'a'", got)
	}
}

func TestFakeBackendNoUsableTokenLoss(t *testing.T) {
	clk := clock.NewFakeClock(time.Time{})
	// Empty tokens with EmitFinish only: closes with no usable token (a loss).
	fb := &FakeBackend{
		BackendName: "empty",
		Clock:       clk,
		Tokens:      nil,
		EmitFinish:  true,
	}
	ch, err := fb.Stream(context.Background(), &oapi.Request{})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch)
	usable := false
	for _, c := range got {
		if c.IsUsable() {
			usable = true
		}
	}
	if usable {
		t.Error("empty backend should produce no usable token")
	}
}

func TestFakeBackendCostAndName(t *testing.T) {
	fb := &FakeBackend{BackendName: "n", Cost: 2.5}
	if fb.Name() != "n" || fb.CostPerRequest() != 2.5 {
		t.Errorf("Name=%q Cost=%v", fb.Name(), fb.CostPerRequest())
	}
}
