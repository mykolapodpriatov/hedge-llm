// Package oapi defines the OpenAI-compatible chat-completion wire types used by
// hedge-llm: the incoming request, the non-streaming response, and the
// streaming SSE chunk. These mirror the public OpenAI Chat Completions schema
// closely enough for hedge-llm to act as a transparent reverse proxy, while
// staying small and dependency-free.
package oapi

import "encoding/json"

// Message is a single chat message in a request or response.
type Message struct {
	// Role is the author role, e.g. "system", "user", or "assistant".
	Role string `json:"role"`
	// Content is the message text. OpenAI also allows structured content; for
	// hedge-llm's transparent-proxy role a string is sufficient.
	Content string `json:"content"`
	// Name optionally identifies the speaker for multi-actor conversations.
	Name string `json:"name,omitempty"`
}

// Request is an OpenAI-compatible /v1/chat/completions request body.
//
// Only the fields hedge-llm needs to inspect (Model, Stream, Messages) are
// typed explicitly. Any additional fields supplied by the client are preserved
// in Extra and re-emitted when forwarding to an upstream backend, so unknown
// knobs (temperature, tools, response_format, …) pass through untouched.
type Request struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	// Stream selects SSE streaming (true) or a single JSON body (false).
	Stream bool `json:"stream,omitempty"`

	// Extra holds every other top-level field from the original request so it
	// can be forwarded verbatim. It is populated by [DecodeRequest].
	Extra map[string]json.RawMessage `json:"-"`
}

// DecodeRequest parses an OpenAI-compatible request body, capturing recognised
// fields and stashing all other top-level fields in Request.Extra so they can
// be forwarded to upstream backends without loss.
func DecodeRequest(data []byte) (*Request, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	req := &Request{Extra: make(map[string]json.RawMessage)}
	for k, v := range raw {
		switch k {
		case "model":
			if err := json.Unmarshal(v, &req.Model); err != nil {
				return nil, err
			}
		case "messages":
			if err := json.Unmarshal(v, &req.Messages); err != nil {
				return nil, err
			}
		case "stream":
			if err := json.Unmarshal(v, &req.Stream); err != nil {
				return nil, err
			}
		default:
			req.Extra[k] = v
		}
	}
	return req, nil
}

// EncodeUpstream serialises the request for sending to an upstream backend,
// overriding the model with upstreamModel and forcing stream=true (hedge-llm
// always streams from upstreams so it can race first tokens, regardless of how
// the client asked to receive the final response). Extra fields are preserved.
func (r *Request) EncodeUpstream(upstreamModel string) ([]byte, error) {
	out := make(map[string]json.RawMessage, len(r.Extra)+3)
	for k, v := range r.Extra {
		out[k] = v
	}
	model, err := json.Marshal(upstreamModel)
	if err != nil {
		return nil, err
	}
	out["model"] = model
	msgs, err := json.Marshal(r.Messages)
	if err != nil {
		return nil, err
	}
	out["messages"] = msgs
	out["stream"] = json.RawMessage("true")
	return json.Marshal(out)
}

// Delta is the incremental content carried by a streaming chunk choice.
type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// ChunkChoice is one choice within a streaming chunk.
type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// Chunk is a single OpenAI streaming chunk (the JSON object that follows a
// `data: ` SSE line). hedge-llm relays these to streaming clients and
// accumulates them for non-streaming clients.
type Chunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`

	// Raw is the exact bytes of the original chunk object as received from the
	// upstream, used to relay streaming responses byte-for-byte. It is not
	// serialised as part of the Chunk itself.
	Raw []byte `json:"-"`
}

// UsableContent returns the first non-empty content delta in the chunk, if any.
// A chunk is "usable" (and may win the hedge race) exactly when this returns a
// non-empty string. Role-only, finish_reason-only, and empty/heartbeat chunks
// return "" and never win.
func (c *Chunk) UsableContent() string {
	for _, ch := range c.Choices {
		if ch.Delta.Content != "" {
			return ch.Delta.Content
		}
	}
	return ""
}

// IsUsable reports whether the chunk carries a non-empty content delta.
func (c *Chunk) IsUsable() bool { return c.UsableContent() != "" }

// ResponseMessage is the assistant message in a non-streaming response.
type ResponseMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Choice is one choice in a non-streaming response.
type Choice struct {
	Index        int             `json:"index"`
	Message      ResponseMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

// Usage reports token accounting for a non-streaming response.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Response is an OpenAI-compatible non-streaming chat-completion response.
type Response struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

// ErrorBody is the OpenAI-compatible error envelope returned on failures.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes a single error.
type ErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
}

// NewError builds an OpenAI-style error envelope.
func NewError(message, typ, code string) ErrorBody {
	return ErrorBody{Error: ErrorDetail{Message: message, Type: typ, Code: code}}
}
