# hedge-llm

> A hedged-request daemon that races multiple LLM providers/models per request and cancels the losers to cut tail latency.

![status](https://img.shields.io/badge/status-active-brightgreen) ![language](https://img.shields.io/badge/language-Go-blue) ![dependencies](https://img.shields.io/badge/dependencies-stdlib%20only-blueviolet) ![license](https://img.shields.io/badge/license-MIT-green)

An OpenAI-compatible reverse-proxy daemon that fires speculative duplicate streaming requests across different providers/models (staggered to control cost), streams the first backend to emit a usable token, and immediately context-cancels the losers.

## Why hedging?

p99 latency — not the average — is what users feel, especially in interactive workloads like voice agents. The classic way to cut a long tail is **request hedging**: send the same request to more than one backend and take whichever responds first. Gateways typically do *sequential* failover (try A, and only on failure try B); hedging instead *races* backends concurrently and cancels the slow ones the moment a winner appears.

That fan-out/cancel pattern is exactly what Go's concurrency model is built for, and getting it right — no leaked goroutines or upstream connections when you cancel mid-stream — is the core of this project.

## How it works

Point your OpenAI client's `base_url` at `hedge-llm`. For each `/v1/chat/completions` request it:

1. Starts the **primary** backend immediately.
2. If no usable token has arrived after `fire_after_ms`, starts the next **backup** backend (subject to `max_in_flight` and `cost_ceiling`). The fire-after timer is a re-armed one-shot, so backups stagger rather than all firing at once.
3. Streams the **first backend to emit a usable token** (the first non-empty content delta — heartbeats and `finish_reason`-only chunks never win) and **context-cancels every other backend**, tearing down their upstream connections.
4. For streaming clients, relays the winner as Server-Sent Events terminated by `data: [DONE]`. For non-streaming clients, accumulates the winner into a single JSON body. **Response headers are written only once a winner is committed**, so if every backend fails the client gets a clean error — never a half-streamed response.

A backend that closes its stream without ever producing a usable token is treated as a **loss**, not a hang: the engine records it and moves on to the next backend, returning an error only if *all* backends lose.

### Correctness guarantees

These are enforced in the engine and exercised directly under `go test -race`:

- **No goroutine or connection leaks.** Each backend runs under its own child context. The engine cancels all children **first**, then waits for them to drain (cancel-before-wait), and every backend producer sends on its channel with a `select { case ch <- chunk: case <-ctx.Done(): return }`, so a loser whose reader has stopped never blocks and always tears down its upstream connection. The HTTP handler completes this drain before returning, so graceful shutdown never races in-flight backends.
- **Exactly one winner.** Winner selection is guarded by a `sync.Once`; after a winner is chosen the relay reads only the winner's stream (and the client's cancellation), never a loser's — so a late loser chunk can never reach the client.
- **Bounds are honored atomically.** A single mutex guards the in-flight count and committed speculative cost; the start decision is a check-and-increment under that lock, so two goroutines can never both observe headroom and both start.
- **Client disconnect is immediate.** A client going away — before or during streaming — cancels every backend and unblocks the engine at once.

## OpenAI-compatible usage

Run the daemon against a config file (see [`config.example.json`](config.example.json)):

```bash
go build -o hedge-llm ./cmd/hedge-llm
./hedge-llm -config config.example.json
```

Then point any OpenAI client at it — only the `base_url` changes:

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1", api_key="unused")

stream = client.chat.completions.create(
    model="gpt-4o-mini",                       # forwarded; each backend uses its own configured model
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True,
)
for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="")
```

Or with `curl`:

```bash
curl http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-4o-mini","stream":true,
       "messages":[{"role":"user","content":"Hello!"}]}'
```

Any extra fields in the request (`temperature`, `tools`, `response_format`, …) pass through to the upstreams unchanged.

### Configuration

Configuration is a JSON file; a few operational knobs can be overridden by environment variables.

```json
{
  "listen_addr": ":8080",
  "backends": [
    {
      "name": "openai",
      "base_url": "https://api.openai.com/v1",
      "api_key_env": "OPENAI_API_KEY",
      "model": "gpt-4o-mini",
      "cost_per_request": 1.0
    },
    {
      "name": "ollama-local",
      "base_url": "http://localhost:11434/v1",
      "model": "llama3.1",
      "cost_per_request": 0.0
    }
  ],
  "policy": { "fire_after_ms": 250, "max_in_flight": 2, "cost_ceiling": 0 },
  "adaptive": { "enabled": false, "window": 128, "min_samples": 10 }
}
```

- Backends are tried in order; the **first is the primary**. API keys are read from the named environment variable and never stored in the file (omit `api_key_env` for keyless upstreams like a local Ollama).
- `v1` targets **OpenAI-compatible** streaming upstreams (OpenAI, Azure OpenAI, vLLM, Ollama's `/v1`, …). Anthropic/Gemini normalization is a documented extension point.

Environment overrides: `HEDGE_LLM_LISTEN_ADDR`, `HEDGE_LLM_FIRE_AFTER_MS`, `HEDGE_LLM_MAX_IN_FLIGHT`, `HEDGE_LLM_COST_CEILING`, `HEDGE_LLM_ADAPTIVE`.

### Policy knobs

| Knob | Meaning |
| --- | --- |
| `fire_after_ms` | How long to wait, after the last backend start, before starting the next backup if no usable token has arrived. |
| `max_in_flight` | Maximum number of concurrent backends per request (the primary counts as one). |
| `cost_ceiling` | Bounds **speculative request starts** — see below. |

### The honest `cost_ceiling` semantics

`cost_ceiling` caps `sum(cost_per_request)` over the backends **started** for a request — i.e. it bounds how many *duplicate requests* may be launched. It is **not** a hard monetary cap on token spend: a long completion's token bill is the upstream's, and hedging cannot bound that. Think of `cost_per_request` as a relative weight (e.g. a pricier model = a higher number), and `cost_ceiling` as "how much speculative duplication am I willing to pay for." With `cost_ceiling: 0` the gate is disabled and only `max_in_flight` limits hedging.

### Adaptive timing

With `adaptive.enabled: true`, the daemon keeps a bounded in-memory ring buffer of each backend's recent first-token latencies and derives **each request's** `fire_after` from the primary's recent p50 — waiting roughly as long as the primary usually takes before hedging. Until the primary has collected `min_samples` samples the static `fire_after` is used; once enough samples exist, its p50 governs when the first backup fires. Statistics are in-memory only (no persistence) and bounded to `window` samples per backend. Off by default (static `fire_after`).

## Metrics

`hedge-llm` exposes Prometheus metrics at `/metrics` (hand-rolled text exposition, no client library dependency):

| Metric | Type | Meaning |
| --- | --- | --- |
| `hedge_requests_total` | counter | Chat-completion requests handled. |
| `hedge_requests_failed_total` | counter | Requests that produced no winner (every backend failed → the client got a 502). |
| `hedge_backend_wins_total{backend}` | counter | Requests won, per backend. |
| `hedge_backend_losses_total{backend,reason}` | counter | Requests lost, per backend, by `reason` (`error`, `no_usable_token`, `canceled`) — so per-backend loss/error rate is computable. Cardinality is bounded to backends × 3. |
| `hedge_redundant_requests_total` | counter | Speculative backups started beyond the primary. |
| `hedge_first_token_latency_seconds` | histogram | First-token latency distribution. |
| `hedge_latency_saved_seconds_total` | counter | Estimated cumulative first-token latency saved by hedging. |
| `hedge_inflight` | gauge | Speculative backends currently in flight (read from the engine's authoritative counter at scrape time). |

A `/healthz` endpoint returns `200 ok` for liveness checks.

## Design & dependencies

- **Zero external dependencies — Go standard library only.** The Prometheus exposition and JSON+env config are hand-rolled, so the build is network-free and the result is a single dependency-free static binary.
- **Deterministic, race-tested.** All timing in tests is driven by an injectable clock and scripted fake backends (no real sleeps), so the hedge race is exercised deterministically. The suite runs under `go test -race`, and leak-freedom is asserted directly against the live goroutine count.

### Package layout

```
cmd/hedge-llm        server entrypoint + graceful shutdown
internal/oapi        OpenAI chat-completion request/response/SSE-chunk types
internal/backend     Backend interface; FakeBackend (deterministic) + HTTP/SSE backend
internal/hedge       the race: start primary, fire backups, select winner, cancel losers
internal/policy      hedge policy (fire-after, max-in-flight, cost-ceiling) + decisions
internal/clock       Clock interface (real + manual fake clock for tests)
internal/adaptive    per-backend rolling latency stats -> suggested fire-after
internal/metrics     registry + hand-rolled Prometheus text exposition
internal/proxy       /v1/chat/completions handler (stream + non-stream)
internal/config      JSON + env config; backend list, policy, costs
```

## Status & roadmap

Implemented and tested:

- [x] OpenAI-compatible streaming + non-streaming proxy
- [x] Speculative cross-provider racing with leak-free loser cancellation
- [x] Policy engine (fire-after, max in-flight, cost ceiling)
- [x] Prometheus metrics + `/metrics`
- [x] Adaptive latency-aware timing

Designed for future work:

- [ ] Per-route cost budgets; shadow/canary mode
- [ ] Anthropic/Gemini normalization adapters
- [ ] Kubernetes sidecar chart

## Development

```bash
go build ./...
gofmt -l .                       # must print nothing
go vet ./...
go test -race -count=1 ./...     # all packages, race detector on
go test -cover ./...
```

## License

[MIT](LICENSE) © 2026 Mykola Podpriatov
