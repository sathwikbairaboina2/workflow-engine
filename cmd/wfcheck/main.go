// Command wfcheck reports nondeterministic code in workflow functions. Run it as
// `go run ./cmd/wfcheck ./...`; it exits non-zero when it finds something.
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/sathwikbairaboina2/workflow-engine/internal/wfcheck"
)

func main() { singlechecker.Main(wfcheck.Analyzer) }
