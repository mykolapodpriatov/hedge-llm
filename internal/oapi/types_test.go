package oapi

import (
	"encoding/json"
	"testing"
)

func TestDecodeRequestPreservesExtra(t *testing.T) {
	body := []byte(`{
		"model": "gpt-4o-mini",
		"messages": [{"role":"user","content":"hi"}],
		"stream": true,
		"temperature": 0.7,
		"tools": [{"type":"function"}]
	}`)
	req, err := DecodeRequest(body)
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if req.Model != "gpt-4o-mini" {
		t.Errorf("Model=%q", req.Model)
	}
	if !req.Stream {
		t.Error("Stream should be true")
	}
	if len(req.Messages) != 1 || req.Messages[0].Content != "hi" {
		t.Errorf("Messages=%+v", req.Messages)
	}
	if _, ok := req.Extra["temperature"]; !ok {
		t.Error("temperature should be preserved in Extra")
	}
	if _, ok := req.Extra["tools"]; !ok {
		t.Error("tools should be preserved in Extra")
	}
	if _, ok := req.Extra["model"]; ok {
		t.Error("model should NOT be in Extra")
	}
}

func TestDecodeRequestInvalidJSON(t *testing.T) {
	if _, err := DecodeRequest([]byte(`{not json`)); err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestEncodeUpstreamOverridesModelAndForcesStream(t *testing.T) {
	req, err := DecodeRequest([]byte(`{
		"model": "client-model",
		"messages": [{"role":"user","content":"hi"}],
		"stream": false,
		"temperature": 0.2
	}`))
	if err != nil {
		t.Fatal(err)
	}
	out, err := req.EncodeUpstream("upstream-model")
	if err != nil {
		t.Fatalf("EncodeUpstream: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	var model string
	_ = json.Unmarshal(got["model"], &model)
	if model != "upstream-model" {
		t.Errorf("upstream model=%q want upstream-model", model)
	}
	var stream bool
	_ = json.Unmarshal(got["stream"], &stream)
	if !stream {
		t.Error("stream must be forced true upstream")
	}
	if _, ok := got["temperature"]; !ok {
		t.Error("temperature should be forwarded")
	}
}

func TestChunkUsableContent(t *testing.T) {
	stop := "stop"
	tests := []struct {
		name  string
		chunk Chunk
		want  string
	}{
		{
			name:  "content delta",
			chunk: Chunk{Choices: []ChunkChoice{{Delta: Delta{Content: "hello"}}}},
			want:  "hello",
		},
		{
			name:  "role only",
			chunk: Chunk{Choices: []ChunkChoice{{Delta: Delta{Role: "assistant"}}}},
			want:  "",
		},
		{
			name:  "finish only",
			chunk: Chunk{Choices: []ChunkChoice{{FinishReason: &stop}}},
			want:  "",
		},
		{
			name:  "empty",
			chunk: Chunk{Choices: []ChunkChoice{{}}},
			want:  "",
		},
		{
			name:  "no choices",
			chunk: Chunk{},
			want:  "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.chunk.UsableContent(); got != tc.want {
				t.Errorf("UsableContent=%q want %q", got, tc.want)
			}
			if tc.chunk.IsUsable() != (tc.want != "") {
				t.Errorf("IsUsable=%v", tc.chunk.IsUsable())
			}
		})
	}
}

func TestNewError(t *testing.T) {
	e := NewError("boom", "server_error", "x")
	if e.Error.Message != "boom" || e.Error.Type != "server_error" || e.Error.Code != "x" {
		t.Errorf("NewError=%+v", e)
	}
}
