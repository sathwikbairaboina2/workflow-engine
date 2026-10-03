package worker

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/internal/wfrt"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// ReplayHistory replays a saved history against workflowFn and returns the first determinism error.
// The commands of a trailing open task are ignored.
func ReplayHistory(history []wire.Event, workflowFn any) error {
	_, err := wfrt.Replay(history, workflowFn)
	return err
}

// ReplayHistoryFile is ReplayHistory for a JSON file holding a []wire.Event or a wire.HistoryResponse
// (what `wf history --json` and the history API return).
func ReplayHistoryFile(path string, workflowFn any) error {
	events, err := ReadHistoryFile(path)
	if err != nil {
		return err
	}
	return ReplayHistory(events, workflowFn)
}

// ReadHistoryFile loads a history saved as a JSON event list or history response.
func ReadHistoryFile(path string) ([]wire.Event, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var events []wire.Event
	if err := json.Unmarshal(data, &events); err == nil {
		return events, nil
	}
	var resp wire.HistoryResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("%s: not a JSON event list or history response: %w", path, err)
	}
	return resp.Events, nil
}
