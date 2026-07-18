package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTemp(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.json")
	if err := os.WriteFile(p, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	path := writeTemp(t, `{
		"listen_addr": ":9090",
		"backends": [
			{"name":"openai","base_url":"https://api.openai.com/v1","api_key_env":"OPENAI_KEY","model":"gpt-4o-mini","cost_per_request":1.0},
			{"name":"ollama","base_url":"http://localhost:11434/v1","model":"llama3","cost_per_request":0.0}
		],
		"policy": {"fire_after_ms": 120, "max_in_flight": 3, "cost_ceiling": 2.5},
		"adaptive": {"enabled": true, "window": 64, "min_samples": 8}
	}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != ":9090" {
		t.Errorf("ListenAddr=%q", cfg.ListenAddr)
	}
	if len(cfg.Backends) != 2 {
		t.Fatalf("backends=%d", len(cfg.Backends))
	}
	if cfg.HedgePolicy().FireAfter != 120*time.Millisecond {
		t.Errorf("FireAfter=%v", cfg.HedgePolicy().FireAfter)
	}
	if !cfg.Adaptive.Enabled || cfg.Adaptive.Window != 64 {
		t.Errorf("adaptive=%+v", cfg.Adaptive)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	path := writeTemp(t, `{
		"listen_addr": ":8080",
		"backends": [{"name":"a","base_url":"http://x/v1","model":"m","cost_per_request":1}],
		"policy": {"fire_after_ms": 250, "max_in_flight": 2}
	}`)
	t.Setenv("HEDGE_LLM_LISTEN_ADDR", ":7000")
	t.Setenv("HEDGE_LLM_FIRE_AFTER_MS", "75")
	t.Setenv("HEDGE_LLM_MAX_IN_FLIGHT", "4")
	t.Setenv("HEDGE_LLM_COST_CEILING", "9.5")
	t.Setenv("HEDGE_LLM_ADAPTIVE", "true")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != ":7000" {
		t.Errorf("env ListenAddr override failed: %q", cfg.ListenAddr)
	}
	if cfg.Policy.FireAfterMS != 75 {
		t.Errorf("env FireAfterMS override failed: %d", cfg.Policy.FireAfterMS)
	}
	if cfg.Policy.MaxInFlight != 4 {
		t.Errorf("env MaxInFlight override failed: %d", cfg.Policy.MaxInFlight)
	}
	if cfg.Policy.CostCeiling != 9.5 {
		t.Errorf("env CostCeiling override failed: %v", cfg.Policy.CostCeiling)
	}
	if !cfg.Adaptive.Enabled {
		t.Error("env adaptive override failed")
	}
}

func TestLoadMalformedEnvOverrides(t *testing.T) {
	const validCfg = `{
		"listen_addr": ":8080",
		"backends": [{"name":"a","base_url":"http://x/v1","model":"m","cost_per_request":1}],
		"policy": {"fire_after_ms": 250, "max_in_flight": 2}
	}`
	tests := []struct {
		name   string
		envKey string
		envVal string
	}{
		{"non-integer fire_after_ms", "HEDGE_LLM_FIRE_AFTER_MS", "abc"},
		{"float fire_after_ms", "HEDGE_LLM_FIRE_AFTER_MS", "12.5"},
		{"non-integer max_in_flight", "HEDGE_LLM_MAX_IN_FLIGHT", "two"},
		{"non-number cost_ceiling", "HEDGE_LLM_COST_CEILING", "notanumber"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTemp(t, validCfg)
			t.Setenv(tc.envKey, tc.envVal)
			if _, err := Load(path); err == nil {
				t.Errorf("expected error for %s=%q, got nil", tc.envKey, tc.envVal)
			}
		})
	}
}

func TestValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"empty addr", Config{ListenAddr: "", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1}}},
		{"no backends", Config{ListenAddr: ":1", Policy: PolicyConfig{MaxInFlight: 1}}},
		{"empty backend name", Config{ListenAddr: ":1", Backends: []BackendConfig{{BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1}}},
		{"dup name", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}, {Name: "a", BaseURL: "u2", Model: "m2"}}, Policy: PolicyConfig{MaxInFlight: 1}}},
		{"empty base_url", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1}}},
		{"empty model", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u"}}, Policy: PolicyConfig{MaxInFlight: 1}}},
		{"neg cost", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m", CostPerRequest: -1}}, Policy: PolicyConfig{MaxInFlight: 1}}},
		{"bad max_in_flight", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 0}}},
		{"neg fire_after", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1, FireAfterMS: -5}}},
		{"neg ceiling", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1, CostCeiling: -1}}},
		{"neg adaptive window", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1}, Adaptive: AdaptiveConfig{Window: -1}}},
		{"neg adaptive min_samples", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1}, Adaptive: AdaptiveConfig{MinSamples: -1}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.cfg.Validate(); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("/nonexistent/path/to/config.json"); err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadBadJSON(t *testing.T) {
	path := writeTemp(t, `{not valid json`)
	if _, err := Load(path); err == nil {
		t.Error("expected error for bad JSON")
	}
}

func TestBuildBackendsResolvesKeys(t *testing.T) {
	t.Setenv("MY_SECRET_KEY", "sk-test-123")
	cfg := Config{
		ListenAddr: ":1",
		Backends: []BackendConfig{
			{Name: "a", BaseURL: "http://x/v1", APIKeyEnv: "MY_SECRET_KEY", Model: "m", CostPerRequest: 1},
			{Name: "b", BaseURL: "http://y/v1", Model: "m2", CostPerRequest: 0},
		},
		Policy: PolicyConfig{MaxInFlight: 2},
	}
	backends := cfg.BuildBackends(nil)
	if len(backends) != 2 {
		t.Fatalf("backends=%d", len(backends))
	}
	if backends[0].Name() != "a" || backends[1].Name() != "b" {
		t.Errorf("names=%q,%q", backends[0].Name(), backends[1].Name())
	}
	if backends[0].CostPerRequest() != 1 {
		t.Errorf("cost=%v", backends[0].CostPerRequest())
	}
}

func TestDefaultConfigHasNoBackends(t *testing.T) {
	d := Default()
	if len(d.Backends) != 0 {
		t.Error("default should have no backends")
	}
	if err := d.Validate(); err == nil {
		t.Error("default config should fail validation (no backends)")
	}
}
