// Package config loads hedge-llm configuration from a JSON file with
// environment-variable overrides, validates it, and builds the runtime backend
// set. The config is hand-parsed with encoding/json (no external deps).
package config

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"hedge-llm/internal/backend"
	"hedge-llm/internal/policy"
)

// BackendConfig describes one upstream backend.
type BackendConfig struct {
	// Name is a stable identifier (metrics labels, logs). Required, unique.
	Name string `json:"name"`
	// BaseURL is the OpenAI-compatible root, e.g. "https://api.openai.com/v1".
	BaseURL string `json:"base_url"`
	// APIKeyEnv names an environment variable holding the API key (the key
	// itself is never stored in the config file). Optional (e.g. local Ollama).
	APIKeyEnv string `json:"api_key_env"`
	// Model is the upstream model name to request.
	Model string `json:"model"`
	// CostPerRequest is the relative speculative cost used by the cost ceiling.
	CostPerRequest float64 `json:"cost_per_request"`
}

// PolicyConfig is the JSON representation of a hedge policy (milliseconds for
// the delay, to keep the file human-friendly).
type PolicyConfig struct {
	FireAfterMS int     `json:"fire_after_ms"`
	MaxInFlight int     `json:"max_in_flight"`
	CostCeiling float64 `json:"cost_ceiling"`
}

// AdaptiveConfig controls adaptive latency-aware timing.
type AdaptiveConfig struct {
	// Enabled turns on adaptive fire-after (off by default → static FireAfter).
	Enabled bool `json:"enabled"`
	// Window is the per-backend latency sample window size (0 → default).
	Window int `json:"window"`
	// MinSamples is how many samples a backend needs before its suggestion is
	// used instead of the static FireAfter (0 → default of 10).
	MinSamples int `json:"min_samples"`
}

// Config is the top-level hedge-llm configuration.
type Config struct {
	// ListenAddr is the HTTP listen address, e.g. ":8080".
	ListenAddr string `json:"listen_addr"`
	// Backends is the ordered list of backends; the first is the primary.
	Backends []BackendConfig `json:"backends"`
	// Policy is the default hedge policy.
	Policy PolicyConfig `json:"policy"`
	// Adaptive configures adaptive timing.
	Adaptive AdaptiveConfig `json:"adaptive"`
}

// Default returns a Config with sensible defaults and no backends.
func Default() Config {
	return Config{
		ListenAddr: ":8080",
		Policy: PolicyConfig{
			FireAfterMS: 250,
			MaxInFlight: 2,
			CostCeiling: 0,
		},
		Adaptive: AdaptiveConfig{Enabled: false, Window: 128, MinSamples: 10},
	}
}

// HedgePolicy converts the JSON policy into a runtime policy.HedgePolicy.
func (c Config) HedgePolicy() policy.HedgePolicy {
	return policy.HedgePolicy{
		FireAfter:   time.Duration(c.Policy.FireAfterMS) * time.Millisecond,
		MaxInFlight: c.Policy.MaxInFlight,
		CostCeiling: c.Policy.CostCeiling,
	}
}

// Load reads, env-overrides, and validates configuration from the given file
// path. If path is empty, defaults plus environment overrides are used (useful
// for fully env-driven deployments, though at least one backend is still
// required to pass validation).
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("hedge-llm: read config %q: %w", path, err)
		}
		// Decode onto the defaults so omitted fields keep their default values.
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("hedge-llm: parse config %q: %w", path, err)
		}
	}
	applyEnvOverrides(&cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyEnvOverrides applies HEDGE_LLM_* environment overrides for the scalar,
// operationally-relevant settings. Backend lists are file-driven.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("HEDGE_LLM_LISTEN_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("HEDGE_LLM_FIRE_AFTER_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Policy.FireAfterMS = n
		}
	}
	if v := os.Getenv("HEDGE_LLM_MAX_IN_FLIGHT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Policy.MaxInFlight = n
		}
	}
	if v := os.Getenv("HEDGE_LLM_COST_CEILING"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.Policy.CostCeiling = f
		}
	}
	if v := os.Getenv("HEDGE_LLM_ADAPTIVE"); v != "" {
		cfg.Adaptive.Enabled = isTruthy(v)
	}
}

func isTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// Validate checks the configuration for operational sanity and returns a clear
// error describing the first problem found.
func (c Config) Validate() error {
	if strings.TrimSpace(c.ListenAddr) == "" {
		return fmt.Errorf("hedge-llm: config: listen_addr must not be empty")
	}
	if len(c.Backends) == 0 {
		return fmt.Errorf("hedge-llm: config: at least one backend is required")
	}
	seen := make(map[string]bool, len(c.Backends))
	for i, b := range c.Backends {
		if strings.TrimSpace(b.Name) == "" {
			return fmt.Errorf("hedge-llm: config: backend[%d] has an empty name", i)
		}
		if seen[b.Name] {
			return fmt.Errorf("hedge-llm: config: duplicate backend name %q", b.Name)
		}
		seen[b.Name] = true
		if strings.TrimSpace(b.BaseURL) == "" {
			return fmt.Errorf("hedge-llm: config: backend %q has an empty base_url", b.Name)
		}
		if strings.TrimSpace(b.Model) == "" {
			return fmt.Errorf("hedge-llm: config: backend %q has an empty model", b.Name)
		}
		if b.CostPerRequest < 0 {
			return fmt.Errorf("hedge-llm: config: backend %q has negative cost_per_request", b.Name)
		}
	}
	if c.Policy.FireAfterMS < 0 {
		return fmt.Errorf("hedge-llm: config: policy.fire_after_ms must be >= 0")
	}
	if c.Policy.MaxInFlight < 1 {
		return fmt.Errorf("hedge-llm: config: policy.max_in_flight must be >= 1")
	}
	if c.Policy.CostCeiling < 0 {
		return fmt.Errorf("hedge-llm: config: policy.cost_ceiling must be >= 0")
	}
	return nil
}

// BuildBackends constructs the runtime backend set from the config, resolving
// API keys from the named environment variables. The supplied http.Client is
// shared by all HTTP backends (nil → each backend builds a default client).
func (c Config) BuildBackends(client *http.Client) []backend.Backend {
	out := make([]backend.Backend, 0, len(c.Backends))
	for _, b := range c.Backends {
		key := ""
		if b.APIKeyEnv != "" {
			key = os.Getenv(b.APIKeyEnv)
		}
		out = append(out, backend.NewHTTPBackend(b.Name, b.BaseURL, key, b.Model, b.CostPerRequest, client))
	}
	return out
}
