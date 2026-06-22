package backend

import (
	"context"
	"time"

	"hedge-llm/internal/clock"
	"hedge-llm/internal/oapi"
)

// FakeBackend is a deterministic [Backend] for tests. All of its timing is
// driven by an injectable [clock.Clock], so tests advance a [clock.FakeClock]
// to control exactly when the first token and subsequent tokens are emitted —
// no real sleeps, no flakiness.
//
// A FakeBackend is configured with a scripted first-token delay, an inter-token
// delay, the list of token strings to emit, and optional error injection
// (either failing to start, or closing the stream early after some tokens).
type FakeBackend struct {
	// BackendName is returned by Name.
	BackendName string
	// Cost is returned by CostPerRequest.
	Cost float64
	// Clock drives all delays. Required (use clock.RealClock{} outside tests).
	Clock clock.Clock

	// FirstTokenDelay is the delay before the first token is sent.
	FirstTokenDelay time.Duration
	// InterTokenDelay is the delay between subsequent tokens.
	InterTokenDelay time.Duration
	// Tokens are the content deltas to emit, in order. Empty strings model
	// heartbeat/keep-alive chunks (which must never win the race).
	Tokens []string

	// StartErr, if non-nil, is returned synchronously by Stream so the request
	// never starts (models an immediate connection failure).
	StartErr error

	// FailAfter, if > 0, makes the producer close the channel after emitting
	// that many tokens (models a mid-stream error / truncated response). When
	// FailAfter is 0 the full Tokens list is emitted then the channel closes
	// normally.
	FailAfter int

	// EmitFinish, when true, appends a final finish_reason="stop" chunk with no
	// content after the tokens, mirroring real OpenAI streams.
	EmitFinish bool

	// OnCancel, if non-nil, is invoked exactly once from a watcher goroutine the
	// moment this stream's context is cancelled (its Done channel closes). It
	// lets tests observe precisely WHEN a backend's context is torn down — e.g.
	// to assert a losing backend is cancelled while the winner is still
	// streaming. It is never called if the stream ends without cancellation.
	OnCancel func()
}

// Name implements Backend.
func (f *FakeBackend) Name() string { return f.BackendName }

// CostPerRequest implements Backend.
func (f *FakeBackend) CostPerRequest() float64 { return f.Cost }

// Stream implements Backend with deterministic, clock-driven timing and obeys
// the cancellation-safe send contract: every send selects on ctx.Done(), and
// the channel is always closed when the producer returns.
func (f *FakeBackend) Stream(ctx context.Context, _ *oapi.Request) (<-chan oapi.Chunk, error) {
	if f.StartErr != nil {
		return nil, f.StartErr
	}
	if f.OnCancel != nil {
		// Watch this stream's context and report the moment it is cancelled. The
		// engine always cancels every backend context by Run return, so this
		// goroutine never leaks.
		go func() {
			<-ctx.Done()
			f.OnCancel()
		}()
	}
	ch := make(chan oapi.Chunk, 1)
	go func() {
		defer close(ch)

		send := func(c oapi.Chunk) bool {
			select {
			case ch <- c:
				return true
			case <-ctx.Done():
				return false
			}
		}

		// Wait for the first-token delay (cancellable).
		if f.FirstTokenDelay > 0 {
			select {
			case <-f.Clock.After(f.FirstTokenDelay):
			case <-ctx.Done():
				return
			}
		}

		limit := len(f.Tokens)
		if f.FailAfter > 0 && f.FailAfter < limit {
			limit = f.FailAfter
		}

		for i := 0; i < limit; i++ {
			if i > 0 && f.InterTokenDelay > 0 {
				select {
				case <-f.Clock.After(f.InterTokenDelay):
				case <-ctx.Done():
					return
				}
			}
			if !send(makeChunk(f.BackendName, f.Tokens[i], nil)) {
				return
			}
		}

		// A truncated stream simply closes here without a finish chunk; the
		// engine treats a close with no usable token as a loss, and a close
		// after some tokens as normal end-of-stream for that backend.
		if f.FailAfter > 0 && f.FailAfter < len(f.Tokens) {
			return
		}

		if f.EmitFinish {
			stop := "stop"
			send(makeChunk(f.BackendName, "", &stop))
		}
	}()
	return ch, nil
}

// makeChunk builds an oapi.Chunk (with a populated Raw payload) for the fake
// backend, so relayed output looks like a real upstream chunk.
func makeChunk(model, content string, finish *string) oapi.Chunk {
	c := oapi.Chunk{
		ID:      "fakecmpl",
		Object:  "chat.completion.chunk",
		Created: 0,
		Model:   model,
		Choices: []oapi.ChunkChoice{{
			Index:        0,
			Delta:        oapi.Delta{Content: content},
			FinishReason: finish,
		}},
	}
	if content != "" {
		c.Choices[0].Delta.Role = "assistant"
	}
	c.Raw = mustMarshalChunk(c)
	return c
}
