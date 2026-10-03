package wfcheck_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/sathwikbairaboina2/workflow-engine/internal/wfcheck"
)

func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), wfcheck.Analyzer, "a")
}
