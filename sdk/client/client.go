// Package client is the HTTP client for wfd, used by applications, workers and the wf CLI.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/internal/naming"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// Client talks to one wfd.
type Client struct {
	base  string
	hc    *http.Client
	retry time.Duration
}

// Option customises a Client.
type Option func(*Client)

// WithHTTPClient replaces the underlying http.Client.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.hc = h } }

// WithRetry sets how long transport errors and 5xx replies are retried (default 30s; 0 disables).
// Backoff starts at 50ms and doubles up to 1s. Long polls are never retried here: the worker loops own that.
func WithRetry(maxElapsed time.Duration) Option { return func(c *Client) { c.retry = maxElapsed } }

// New returns a client for the server at baseURL.
func New(baseURL string, opts ...Option) *Client {
	c := &Client{base: strings.TrimRight(baseURL, "/"), hc: &http.Client{}, retry: 30 * time.Second}
	for _, o := range opts {
		o(c)
	}
	return c
}

// StartOptions identify the workflow to start.
type StartOptions struct{ ID, TaskQueue string }

// roundTrip performs one request. body/out may be nil. A 204 leaves out untouched and returns (false, nil).
func (c *Client) roundTrip(ctx context.Context, method, path string, body, out any) (got bool, err error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return false, fmt.Errorf("client: encode request: %w", err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return false, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	switch {
	case resp.StatusCode == http.StatusNoContent:
		return false, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if out == nil {
			return true, nil
		}
		if err := json.Unmarshal(data, out); err != nil {
			return false, fmt.Errorf("client: decode response: %w", err)
		}
		return true, nil
	}
	ae := &APIError{Status: resp.StatusCode, Code: "http_error", Message: strings.TrimSpace(string(data))}
	var eb wire.ErrorBody
	if json.Unmarshal(data, &eb) == nil && eb.Error.Code != "" {
		ae.Code, ae.Message, ae.RunID = eb.Error.Code, eb.Error.Message, eb.Error.RunID
	}
	return false, ae
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	if ae, ok := err.(*APIError); ok {
		return ae.Status >= 500
	}
	return true // transport error
}

// call is roundTrip with the retry budget.
func (c *Client) call(ctx context.Context, path string, body, out any) (bool, error) {
	deadline := time.Now().Add(c.retry)
	backoff := 50 * time.Millisecond
	for {
		got, err := c.roundTrip(ctx, http.MethodPost, path, body, out)
		if err == nil || !retryable(err) || ctx.Err() != nil || c.retry <= 0 || time.Now().Add(backoff).After(deadline) {
			return got, err
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return false, err
		}
		backoff = min(backoff*2, time.Second)
	}
}

// Start begins a workflow. workflow is a registered name or the workflow function itself.
func (c *Client) Start(ctx context.Context, o StartOptions, workflow any, input any) (string, error) {
	p, err := wire.Encode(input)
	if err != nil {
		return "", err
	}
	var resp wire.StartResponse
	if _, err := c.call(ctx, "/v1/workflows/start", wire.StartRequest{
		WorkflowID: o.ID, WorkflowType: naming.Name(workflow), TaskQueue: o.TaskQueue, Input: p}, &resp); err != nil {
		return "", err
	}
	return resp.RunID, nil
}

// Signal delivers a signal to the open run of workflowID.
func (c *Client) Signal(ctx context.Context, workflowID, name string, payload any) error {
	p, err := wire.Encode(payload)
	if err != nil {
		return err
	}
	_, err = c.call(ctx, "/v1/workflows/signal", wire.SignalRequest{WorkflowID: workflowID, Name: name, Payload: p}, nil)
	return err
}

// Cancel requests cancellation of the open run of workflowID.
func (c *Client) Cancel(ctx context.Context, workflowID, reason string) error {
	_, err := c.call(ctx, "/v1/workflows/cancel", wire.CancelRequest{WorkflowID: workflowID, Reason: reason}, nil)
	return err
}

// Describe returns the latest run of workflowID.
func (c *Client) Describe(ctx context.Context, workflowID string) (wire.DescribeResponse, error) {
	var out wire.DescribeResponse
	_, err := c.call(ctx, "/v1/workflows/describe", wire.DescribeRequest{WorkflowID: workflowID}, &out)
	return out, err
}

// History returns every event of a run, following pages of 1000. An empty runID means the latest run.
func (c *Client) History(ctx context.Context, workflowID, runID string) ([]wire.Event, error) {
	var all []wire.Event
	from := int64(1)
	for {
		var page wire.HistoryResponse
		if _, err := c.call(ctx, "/v1/workflows/history", wire.HistoryRequest{WorkflowID: workflowID, RunID: runID, FromEventID: from, PageSize: 1000}, &page); err != nil {
			return nil, err
		}
		runID = page.RunID // later pages must stay on the run the first page resolved
		all = append(all, page.Events...)
		if page.NextEventID == 0 {
			return all, nil
		}
		from = page.NextEventID
	}
}

// GetResult waits for the workflow to close and decodes its result into out.
func (c *Client) GetResult(ctx context.Context, workflowID string, out any) error {
	for {
		d, err := c.Describe(ctx, workflowID)
		if err != nil {
			return err
		}
		switch d.Status {
		case "completed":
			return d.Result.Decode(out)
		case "failed":
			f := wire.Failure{Type: "Unknown"}
			if d.Failure != nil {
				f = *d.Failure
			}
			return &WorkflowFailedError{Failure: f}
		case "canceled":
			return ErrWorkflowCanceled
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Health returns nil when the server answers /healthz.
func (c *Client) Health(ctx context.Context) error {
	_, err := c.roundTrip(ctx, http.MethodGet, "/healthz", nil, nil)
	return err
}

// PollWorkflowTask long-polls for a workflow task; nil when the poll timed out.
func (c *Client) PollWorkflowTask(ctx context.Context, queue, identity string) (*wire.WorkflowTask, error) {
	var t wire.WorkflowTask
	got, err := c.roundTrip(ctx, http.MethodPost, "/v1/tasks/workflow/poll", wire.PollRequest{Queue: queue, Identity: identity}, &t)
	if err != nil || !got {
		return nil, err
	}
	return &t, nil
}

// CompleteWorkflowTask returns a workflow step's commands.
func (c *Client) CompleteWorkflowTask(ctx context.Context, req wire.CompleteWorkflowTaskRequest) error {
	_, err := c.call(ctx, "/v1/tasks/workflow/complete", req, nil)
	return err
}

// FailWorkflowTask reports that a workflow step could not run.
func (c *Client) FailWorkflowTask(ctx context.Context, req wire.FailWorkflowTaskRequest) error {
	_, err := c.call(ctx, "/v1/tasks/workflow/fail", req, nil)
	return err
}

// PollActivityTask long-polls for an activity task; nil when the poll timed out.
func (c *Client) PollActivityTask(ctx context.Context, queue, identity string) (*wire.ActivityTask, error) {
	var t wire.ActivityTask
	got, err := c.roundTrip(ctx, http.MethodPost, "/v1/tasks/activity/poll", wire.PollRequest{Queue: queue, Identity: identity}, &t)
	if err != nil || !got {
		return nil, err
	}
	return &t, nil
}

// CompleteActivityTask reports a successful activity attempt.
func (c *Client) CompleteActivityTask(ctx context.Context, req wire.CompleteActivityTaskRequest) error {
	_, err := c.call(ctx, "/v1/tasks/activity/complete", req, nil)
	return err
}

// FailActivityTask reports a failed activity attempt.
func (c *Client) FailActivityTask(ctx context.Context, req wire.FailActivityTaskRequest) error {
	_, err := c.call(ctx, "/v1/tasks/activity/fail", req, nil)
	return err
}
