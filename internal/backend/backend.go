// Package backend defines the Backend abstraction that the hedge engine races,
// plus two concrete implementations: a deterministic FakeBackend for tests and
// a generic httpBackend for OpenAI-compatible upstreams.
//
// # Cancellation-safe send contract (mandatory)
//
// Every Backend.Stream implementation MUST obey this contract, because the
// hedge engine stops reading from losing backends the instant a winner is
// chosen:
//
//   - The producer goroutine sends on the result channel using
//     select { case ch <- chunk: case <-ctx.Done(): return } for EVERY send,
//     so a loser whose reader has stopped never blocks on send.
//   - On return (ctx cancel, error, or upstream EOF) the producer closes the
//     channel and releases any upstream connection (defer resp.Body.Close()).
//
// Correctness does not depend on the channel buffer size; it depends on the
// ctx.Done() case in every send. This is what makes hedging leak-free.
package backend

import (
	"context"

	"hedge-llm/internal/oapi"
)

// Backend is a single LLM provider/model endpoint the engine can race.
type Backend interface {
	// Name is a stable identifier used in metrics and logs.
	Name() string

	// CostPerRequest is the relative speculative cost of issuing one request to
	// this backend. It is unitless and only compared against a policy's
	// CostCeiling; it does NOT represent token spend.
	CostPerRequest() float64

	// Stream issues the request and returns a channel of streamed chunks. It
	// MUST obey the cancellation-safe send contract documented on this package:
	// every send selects on ctx.Done(), and the channel is closed (and any
	// upstream connection released) when the producer goroutine returns.
	//
	// An error returned synchronously means the request could not be started at
	// all; in that case no channel is returned. Errors that occur mid-stream
	// are surfaced by closing the channel (the engine treats a close with no
	// usable token as a loss).
	Stream(ctx context.Context, req *oapi.Request) (<-chan oapi.Chunk, error)
}
