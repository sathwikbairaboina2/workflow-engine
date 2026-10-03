package naming

import "testing"

type ledger struct{}

func (*ledger) Debit() {}

func Debit() {}

func TestName(t *testing.T) {
	if got := Name(Debit); got != "Debit" {
		t.Errorf("plain func: %q", got)
	}
	l := &ledger{}
	if got := Name(l.Debit); got != "Debit" {
		t.Errorf("method value: %q", got)
	}
	if got := Name("Custom"); got != "Custom" {
		t.Errorf("string: %q", got)
	}
}
