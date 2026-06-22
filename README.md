# hedge-llm

> A hedged-request daemon that races multiple LLM providers/models per request and cancels the losers to cut tail latency.

![status](https://img.shields.io/badge/status-early%20development-orange) ![language](https://img.shields.io/badge/language-Go-blue) ![license](https://img.shields.io/badge/license-MIT-green)

An OpenAI-compatible reverse-proxy daemon that fires speculative duplicate streaming requests across different providers/models (staggered to control cost), streams the first backend to emit a usable token, and immediately context-cancels the losers.

## Why

p99 latency, not the average, is what users feel. Gateways do sequential failover; this races backends and cancels the slow ones — exactly the fan-out/cancel problem Go's concurrency model is built for.

## Features

- OpenAI-compatible endpoint — clients just point at it
- Per-request speculative duplicate streaming across providers, with loser context-cancellation on first token
- Hedging policy knobs: fire-after-Xms, max in-flight copies, per-policy cost ceiling
- Adaptive mode that learns per-backend latency distributions
- Prometheus metrics: latency saved, redundant spend, per-backend win-rate

## How it works

Point your OpenAI client at hedge-llm. Per request it launches staggered duplicates across configured backends, streams whichever responds first, cancels the rest, and exports the latency/cost tradeoff as Prometheus metrics.

## Tech stack

- Go
- net/http
- context cancellation
- Prometheus client
- OpenAI / Anthropic / Gemini / Ollama HTTP APIs

## Status & roadmap

🚧 **Early development.** This repository is being built in the open; the scaffold and design are in place and the implementation is landing incrementally.

- [ ] OpenAI-compatible streaming proxy with single backend
- [ ] Speculative cross-provider racing + loser cancellation
- [ ] Policy engine (fire-after, max copies, cost ceiling) + Prometheus metrics
- [ ] Adaptive latency-aware timing; Kubernetes sidecar chart

## Installation

> Coming soon.

## License

[MIT](LICENSE) © 2026 Mykola Podpriatov
