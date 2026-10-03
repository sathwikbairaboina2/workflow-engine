package activity

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIdempotencyKeyFormat(t *testing.T) {
	if got := Key("r1", 5); got != "r1:5" {
		t.Fatalf("key %q", got)
	}
}

func TestInfoRoundTrip(t *testing.T) {
	ctx := WithInfo(context.Background(), Info{RunID: "r", Attempt: 3})
	if got := GetInfo(ctx); got.RunID != "r" || got.Attempt != 3 {
		t.Fatalf("info %+v", got)
	}
	if got := GetInfo(context.Background()); got.RunID != "" {
		t.Fatalf("zero info expected, got %+v", got)
	}
}

func TestToFailure(t *testing.T) {
	if f := ToFailure(errors.New("plain")); f.Type != "GenericError" || f.Message != "plain" || f.NonRetryable {
		t.Errorf("plain: %+v", f)
	}
	if f := ToFailure(NewNonRetryableError("Fatal", "x")); f.Type != "Fatal" || !f.NonRetryable {
		t.Errorf("non-retryable: %+v", f)
	}
	if f := ToFailure(NewError("Soft", "y")); f.Type != "Soft" || f.NonRetryable {
		t.Errorf("retryable: %+v", f)
	}
	wrapped := fmt.Errorf("while debiting: %w", NewNonRetryableError("Fatal", "z"))
	if f := ToFailure(wrapped); f.Type != "Fatal" || !f.NonRetryable {
		t.Errorf("wrapped: %+v", f)
	}
}
