package config

import (
	"os"
	"path/filepath"
	"strings"
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
		"policy": {"fire_after_ms": 120, "max_in_flight": 3, "cost_ceiling": 2.5, "request_timeout_ms": 1500, "loss_cooldown_n": 3, "loss_cooldown_ms": 5000},
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
	if cfg.Policy.RequestTimeoutMS != 1500 {
		t.Errorf("RequestTimeoutMS=%d", cfg.Policy.RequestTimeoutMS)
	}
	if cfg.HedgePolicy().RequestTimeout != 1500*time.Millisecond {
		t.Errorf("RequestTimeout=%v", cfg.HedgePolicy().RequestTimeout)
	}
	if cfg.Policy.LossCooldownN != 3 || cfg.Policy.LossCooldownMS != 5000 {
		t.Errorf("loss cooldown config=%+v", cfg.Policy)
	}
	if cfg.HedgePolicy().LossCooldownN != 3 || cfg.HedgePolicy().LossCooldown != 5*time.Second {
		t.Errorf("LossCooldown policy=%+v", cfg.HedgePolicy())
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
	t.Setenv("HEDGE_LLM_REQUEST_TIMEOUT_MS", "800")
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
	if cfg.Policy.RequestTimeoutMS != 800 {
		t.Errorf("env RequestTimeoutMS override failed: %d", cfg.Policy.RequestTimeoutMS)
	}
	if !cfg.Adaptive.Enabled {
		t.Error("env adaptive override failed")
	}
}

func TestLoadListenAPIKeyEnvField(t *testing.T) {
	path := writeTemp(t, `{
		"listen_addr": ":8080",
		"backends": [{"name":"a","base_url":"http://x/v1","model":"m","cost_per_request":1}],
		"policy": {"fire_after_ms": 250, "max_in_flight": 2},
		"listen_api_key_env": "MY_INBOUND_KEY_VAR"
	}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAPIKeyEnv != "MY_INBOUND_KEY_VAR" {
		t.Errorf("ListenAPIKeyEnv=%q want MY_INBOUND_KEY_VAR", cfg.ListenAPIKeyEnv)
	}
}

func TestListenAPIKeyResolvesFromNamedEnvVar(t *testing.T) {
	t.Setenv("MY_INBOUND_KEY_VAR", "sk-inbound-secret")
	cfg := Config{ListenAPIKeyEnv: "MY_INBOUND_KEY_VAR"}
	if got := cfg.ListenAPIKey(); got != "sk-inbound-secret" {
		t.Errorf("ListenAPIKey()=%q want sk-inbound-secret", got)
	}
}

func TestListenAPIKeyDisabledByDefault(t *testing.T) {
	cfg := Default()
	if got := cfg.ListenAPIKey(); got != "" {
		t.Errorf("ListenAPIKey()=%q want empty (auth disabled by default)", got)
	}
}

func TestListenAPIKeyEmptyWhenNamedVarUnset(t *testing.T) {
	cfg := Config{ListenAPIKeyEnv: "HEDGE_LLM_TEST_DEFINITELY_UNSET_VAR"}
	if got := cfg.ListenAPIKey(); got != "" {
		t.Errorf("ListenAPIKey()=%q want empty when named env var is unset", got)
	}
}

func TestLoadEnvOverrideAPIKey(t *testing.T) {
	path := writeTemp(t, `{
		"listen_addr": ":8080",
		"backends": [{"name":"a","base_url":"http://x/v1","model":"m","cost_per_request":1}],
		"policy": {"fire_after_ms": 250, "max_in_flight": 2}
	}`)
	t.Setenv("HEDGE_LLM_API_KEY", "sk-override-secret")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAPIKeyEnv != "HEDGE_LLM_API_KEY" {
		t.Errorf("ListenAPIKeyEnv=%q want HEDGE_LLM_API_KEY", cfg.ListenAPIKeyEnv)
	}
	if got := cfg.ListenAPIKey(); got != "sk-override-secret" {
		t.Errorf("ListenAPIKey()=%q want sk-override-secret", got)
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
		{"non-integer request_timeout_ms", "HEDGE_LLM_REQUEST_TIMEOUT_MS", "abc"},
		{"float request_timeout_ms", "HEDGE_LLM_REQUEST_TIMEOUT_MS", "12.5"},
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
		{"neg request_timeout", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1, RequestTimeoutMS: -1}}},
		{"neg loss_cooldown_n", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1, LossCooldownN: -1}}},
		{"neg loss_cooldown_ms", Config{ListenAddr: ":1", Backends: []BackendConfig{{Name: "a", BaseURL: "u", Model: "m"}}, Policy: PolicyConfig{MaxInFlight: 1, LossCooldownMS: -1}}},
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

// ---- per-model policy overrides --------------------------------------------

// baseConfigWithOverrides returns a minimal valid config carrying the given
// overrides, so each test below only has to state what it is checking.
func baseConfigWithOverrides(ovs map[string]PolicyOverride) Config {
	c := Default()
	c.Backends = []BackendConfig{{Name: "a", BaseURL: "http://x/v1", Model: "m"}}
	c.Policy = PolicyConfig{FireAfterMS: 250, MaxInFlight: 2, CostCeiling: 0, RequestTimeoutMS: 0}
	c.PolicyOverrides = ovs
	return c
}

func intPtr(v int) *int           { return &v }
func floatPtr(v float64) *float64 { return &v }

// A partial override changes only the fields it names.
func TestHedgePolicyForMergesPartialOverride(t *testing.T) {
	c := baseConfigWithOverrides(map[string]PolicyOverride{
		"fast": {FireAfterMS: intPtr(120)},
	})
	got := c.HedgePolicyFor("fast")
	if got.FireAfter != 120*time.Millisecond {
		t.Errorf("FireAfter=%v, want 120ms", got.FireAfter)
	}
	if got.MaxInFlight != 2 {
		t.Errorf("MaxInFlight=%d, want the default 2", got.MaxInFlight)
	}
}

// Every overridable knob round-trips.
func TestHedgePolicyForOverridesEveryKnob(t *testing.T) {
	c := baseConfigWithOverrides(map[string]PolicyOverride{
		"tuned": {
			FireAfterMS:      intPtr(2000),
			MaxInFlight:      intPtr(1),
			CostCeiling:      floatPtr(2.5),
			RequestTimeoutMS: intPtr(30000),
		},
	})
	got := c.HedgePolicyFor("tuned")
	if got.FireAfter != 2*time.Second {
		t.Errorf("FireAfter=%v, want 2s", got.FireAfter)
	}
	if got.MaxInFlight != 1 {
		t.Errorf("MaxInFlight=%d, want 1", got.MaxInFlight)
	}
	if got.CostCeiling != 2.5 {
		t.Errorf("CostCeiling=%v, want 2.5", got.CostCeiling)
	}
	if got.RequestTimeout != 30*time.Second {
		t.Errorf("RequestTimeout=%v, want 30s", got.RequestTimeout)
	}
}

// An explicit zero is a real value, not "inherit the default".
func TestHedgePolicyForExplicitZeroDisablesGate(t *testing.T) {
	c := baseConfigWithOverrides(map[string]PolicyOverride{
		"open": {CostCeiling: floatPtr(0)},
	})
	c.Policy.CostCeiling = 5
	if got := c.HedgePolicyFor("open").CostCeiling; got != 0 {
		t.Errorf("CostCeiling=%v, want 0 (explicitly disabled)", got)
	}
	if got := c.HedgePolicyFor("other").CostCeiling; got != 5 {
		t.Errorf("unlisted model CostCeiling=%v, want the default 5", got)
	}
}

// An unlisted model, and a config with no overrides at all, use the default.
func TestHedgePolicyForFallsBackToDefault(t *testing.T) {
	withOverrides := baseConfigWithOverrides(map[string]PolicyOverride{
		"fast": {FireAfterMS: intPtr(120)},
	})
	none := baseConfigWithOverrides(nil)
	want := none.HedgePolicy()
	if got := withOverrides.HedgePolicyFor("unknown"); got != want {
		t.Errorf("unknown model: %+v, want the default %+v", got, want)
	}
	if got := none.HedgePolicyFor("anything"); got != want {
		t.Errorf("no overrides configured: %+v, want %+v", got, want)
	}
	if none.HasPolicyOverrides() {
		t.Error("HasPolicyOverrides() = true with no overrides configured")
	}
	if !withOverrides.HasPolicyOverrides() {
		t.Error("HasPolicyOverrides() = false with one override configured")
	}
}

// The merged override is held to the same bounds as the default policy.
func TestValidateRejectsBadOverride(t *testing.T) {
	for _, tc := range []struct {
		name string
		ovs  map[string]PolicyOverride
		want string
	}{
		{"max_in_flight below one", map[string]PolicyOverride{"m": {MaxInFlight: intPtr(0)}}, `policy_overrides["m"].max_in_flight`},
		{"negative fire_after", map[string]PolicyOverride{"m": {FireAfterMS: intPtr(-1)}}, `policy_overrides["m"].fire_after_ms`},
		{"negative cost_ceiling", map[string]PolicyOverride{"m": {CostCeiling: floatPtr(-0.5)}}, `policy_overrides["m"].cost_ceiling`},
		{"negative request_timeout", map[string]PolicyOverride{"m": {RequestTimeoutMS: intPtr(-5)}}, `policy_overrides["m"].request_timeout_ms`},
		{"empty model key", map[string]PolicyOverride{"": {FireAfterMS: intPtr(10)}}, "empty model key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := baseConfigWithOverrides(tc.ovs).Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want an error mentioning %s", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate() = %v, want it to mention %s", err, tc.want)
			}
		})
	}
}

// A valid override passes validation.
func TestValidateAcceptsGoodOverride(t *testing.T) {
	c := baseConfigWithOverrides(map[string]PolicyOverride{
		"o3":          {FireAfterMS: intPtr(2000), MaxInFlight: intPtr(1), CostCeiling: floatPtr(2)},
		"gpt-4o-mini": {FireAfterMS: intPtr(120), MaxInFlight: intPtr(3)},
	})
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

// The key survives a JSON round-trip, which is what -print-config emits.
func TestPolicyOverridesLoadFromJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	body := `{
	  "backends": [{"name": "a", "base_url": "http://x/v1", "model": "m"}],
	  "policy": {"fire_after_ms": 250, "max_in_flight": 2},
	  "policy_overrides": {"o3": {"fire_after_ms": 2000, "max_in_flight": 1}}
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() = %v", err)
	}
	if got := cfg.HedgePolicyFor("o3"); got.FireAfter != 2*time.Second || got.MaxInFlight != 1 {
		t.Errorf("o3 policy = %+v, want fire_after 2s / max_in_flight 1", got)
	}
	if got := cfg.HedgePolicyFor("other").FireAfter; got != 250*time.Millisecond {
		t.Errorf("unlisted model fire_after = %v, want 250ms", got)
	}
}
