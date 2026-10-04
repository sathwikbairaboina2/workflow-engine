// Package workflowengine is a small durable workflow engine: event-sourced history in SQLite,
// deterministic replay in the Go SDK, and a kill -9 chaos harness. The importable packages live under
// sdk/ (client, worker, workflow, activity) and wire/; the server is cmd/wfd. See README.md.
package workflowengine
