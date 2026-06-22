// Command hedge-llm is an OpenAI-compatible hedging reverse-proxy daemon. It
// races speculative duplicate streaming requests across configured backends,
// streams the first backend to emit a usable token, cancels the losers, and
// exports Prometheus metrics.
//
// Usage:
//
//	hedge-llm -config config.json
//
// Configuration is a JSON file (see internal/config) with HEDGE_LLM_*
// environment overrides for operational knobs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"hedge-llm/internal/adaptive"
	"hedge-llm/internal/clock"
	"hedge-llm/internal/config"
	"hedge-llm/internal/hedge"
	"hedge-llm/internal/metrics"
	"hedge-llm/internal/proxy"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("hedge-llm: %v", err)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to JSON config file (HEDGE_LLM_* env vars override scalars)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	// One shared HTTP client (no overall timeout — streaming responses are
	// long-lived; per-request lifetime is governed by the request context).
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}

	backends := cfg.BuildBackends(httpClient)
	engine := hedge.NewEngine(backends, cfg.HedgePolicy(), clock.RealClock{})

	reg := metrics.NewRegistry(nil)
	// Single source of truth for the inflight gauge: the engine's mutex-guarded
	// counter, read at scrape time.
	reg.SetInFlightFunc(engine.InFlight)

	opts := []proxy.Option{proxy.WithMetrics(reg)}
	if cfg.Adaptive.Enabled {
		est := adaptive.NewEstimator(cfg.Adaptive.Window)
		opts = append(opts, proxy.WithLatencyObserver(est))
		log.Printf("hedge-llm: adaptive timing enabled (window=%d)", cfg.Adaptive.Window)
	}
	handler := proxy.NewHandler(engine, opts...)

	mux := http.NewServeMux()
	mux.Handle("/v1/chat/completions", handler)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = reg.WriteTo(w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("hedge-llm: listening on %s with %d backend(s)", cfg.ListenAddr, len(backends))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Printf("hedge-llm: shutdown signal received, draining…")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		log.Printf("hedge-llm: shutdown complete")
		return nil
	}
}
