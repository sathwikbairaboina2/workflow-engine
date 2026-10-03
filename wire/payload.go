// Package wire holds the types shared by the server and the SDK: events, commands, payloads and API bodies.
package wire

import (
	"encoding/json"
	"fmt"
)

// Payload is an encoded user value.
type Payload struct {
	Encoding string          `json:"encoding"`
	Data     json.RawMessage `json:"data,omitempty"`
}

// Encode wraps v as a JSON payload. A nil v yields (nil, nil).
func Encode(v any) (*Payload, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("wire: encode payload: %w", err)
	}
	return &Payload{Encoding: "json", Data: b}, nil
}

// MustEncode is Encode that panics on error; for tests and constants only.
func MustEncode(v any) *Payload {
	p, err := Encode(v)
	if err != nil {
		panic(err)
	}
	return p
}

// Decode unmarshals the payload into v. A nil payload or nil v is a no-op.
func (p *Payload) Decode(v any) error {
	if p == nil || v == nil {
		return nil
	}
	if p.Encoding != "json" {
		return fmt.Errorf("wire: unsupported encoding %q", p.Encoding)
	}
	if len(p.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(p.Data, v); err != nil {
		return fmt.Errorf("wire: decode payload: %w", err)
	}
	return nil
}

// Size returns the payload's data length in bytes.
func (p *Payload) Size() int {
	if p == nil {
		return 0
	}
	return len(p.Data)
}

// Failure describes why an activity or workflow failed.
type Failure struct {
	Type         string `json:"type"`
	Message      string `json:"message"`
	NonRetryable bool   `json:"non_retryable,omitempty"`
}

// RetryPolicy is the server-side activity retry policy.
type RetryPolicy struct {
	InitialMS    int64    `json:"initial_ms"`
	Backoff      float64  `json:"backoff"`
	MaxMS        int64    `json:"max_ms"`
	MaxAttempts  int      `json:"max_attempts"` // 0 = unlimited
	NonRetryable []string `json:"non_retryable,omitempty"`
}
