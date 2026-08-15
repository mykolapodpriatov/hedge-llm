package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hedge-llm/internal/config"
)

// writeConfig writes a minimal valid config file and returns its path.
func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.json")
	if err := os.WriteFile(p, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRunVersion(t *testing.T) {
	var buf bytes.Buffer
	// -version returns before any config load, so no -config is needed.
	if err := run([]string{"-version"}, &buf); err != nil {
		t.Fatalf("run(-version): %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got != version {
		t.Errorf("version output=%q want %q", got, version)
	}
}

func TestRunPrintConfig(t *testing.T) {
	path := writeConfig(t, `{
		"listen_addr": ":9090",
		"backends": [
			{"name":"openai","base_url":"https://api.openai.com/v1","api_key_env":"OPENAI_KEY","model":"gpt-4o-mini","cost_per_request":1.0}
		],
		"policy": {"fire_after_ms": 120, "max_in_flight": 3, "cost_ceiling": 2.5},
		"adaptive": {"enabled": true, "window": 64, "min_samples": 8}
	}`)

	var buf bytes.Buffer
	if err := run([]string{"-config", path, "-print-config"}, &buf); err != nil {
		t.Fatalf("run(-print-config): %v", err)
	}

	var got config.Config
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if got.ListenAddr != ":9090" {
		t.Errorf("ListenAddr=%q want :9090", got.ListenAddr)
	}
	if len(got.Backends) != 1 || got.Backends[0].Name != "openai" {
		t.Errorf("backends=%+v", got.Backends)
	}
	if got.Policy.FireAfterMS != 120 || got.Policy.MaxInFlight != 3 {
		t.Errorf("policy=%+v", got.Policy)
	}
	// API keys must never be present in the dumped config (they live in env).
	if strings.Contains(buf.String(), "sk-") {
		t.Errorf("printed config appears to contain a secret:\n%s", buf.String())
	}
}

func TestRunValidateOK(t *testing.T) {
	path := writeConfig(t, `{
		"listen_addr": ":9090",
		"backends": [
			{"name":"openai","base_url":"https://api.openai.com/v1","api_key_env":"OPENAI_KEY","model":"gpt-4o-mini","cost_per_request":1.0}
		],
		"policy": {"fire_after_ms": 120, "max_in_flight": 3, "cost_ceiling": 2.5}
	}`)

	var buf bytes.Buffer
	if err := run([]string{"-config", path, "-validate"}, &buf); err != nil {
		t.Fatalf("run(-validate): %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got != "config OK" {
		t.Errorf("output=%q want %q", got, "config OK")
	}
	if strings.Contains(buf.String(), "listening") {
		t.Errorf("-validate must not start the daemon: %q", buf.String())
	}
}

func TestRunValidateInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"missing backend", `{"listen_addr": ":8080"}`},
		{"bad max_in_flight", `{
			"listen_addr": ":8080",
			"backends": [{"name":"a","base_url":"http://x/v1","model":"m","cost_per_request":1}],
			"policy": {"fire_after_ms": 250, "max_in_flight": 0}
		}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.body)
			var buf bytes.Buffer
			if err := run([]string{"-config", path, "-validate"}, &buf); err == nil {
				t.Fatal("expected validation error")
			}
			if strings.Contains(buf.String(), "listening") {
				t.Errorf("output must not contain listening, got %q", buf.String())
			}
		})
	}
}

func TestRunValidateExclusiveFlags(t *testing.T) {
	path := writeConfig(t, `{
		"listen_addr": ":8080",
		"backends": [{"name":"a","base_url":"http://x/v1","model":"m","cost_per_request":1}],
		"policy": {"fire_after_ms": 250, "max_in_flight": 2}
	}`)
	for _, extra := range []string{"-version", "-print-config"} {
		t.Run(extra, func(t *testing.T) {
			var buf bytes.Buffer
			if err := run([]string{"-config", path, "-validate", extra}, &buf); err == nil {
				t.Fatalf("expected usage error for -validate %s", extra)
			}
		})
	}
}

func TestRunPrintConfigInvalidConfigErrors(t *testing.T) {
	// A config that fails validation (no backends) must surface an error rather
	// than printing anything.
	path := writeConfig(t, `{"listen_addr": ":8080"}`)
	var buf bytes.Buffer
	if err := run([]string{"-config", path, "-print-config"}, &buf); err == nil {
		t.Error("expected error for invalid config, got nil")
	}
	if buf.Len() != 0 {
		t.Errorf("expected no output on error, got %q", buf.String())
	}
}
