package wfrt

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

var (
	contextType = reflect.TypeOf((*Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

// ValidateWorkflowFunc accepts func(Context, T) (R, error) and func(Context, T) error only.
func ValidateWorkflowFunc(fn any) error {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func {
		return fmt.Errorf("workflow must be a function, got %T", fn)
	}
	if t.NumIn() != 2 || t.In(0) != contextType {
		return fmt.Errorf("workflow %s must take (workflow.Context, input)", t)
	}
	switch t.NumOut() {
	case 1:
		if t.Out(0) != errorType {
			return fmt.Errorf("workflow %s must return error or (result, error)", t)
		}
	case 2:
		if t.Out(1) != errorType {
			return fmt.Errorf("workflow %s must return error as its last result", t)
		}
	default:
		return fmt.Errorf("workflow %s must return error or (result, error)", t)
	}
	return nil
}

// invokeWorkflow decodes the input, calls fn and encodes its result. A panic propagates to the dispatcher.
func invokeWorkflow(fn any, ctx Context, input *wire.Payload) (*wire.Payload, error) {
	ft := reflect.TypeOf(fn)
	in := reflect.New(ft.In(1))
	if err := input.Decode(in.Interface()); err != nil {
		return nil, fmt.Errorf("decode workflow input: %w", err)
	}
	out := reflect.ValueOf(fn).Call([]reflect.Value{reflect.ValueOf(ctx), in.Elem()})
	var err error
	if e := out[len(out)-1]; !e.IsNil() {
		err = e.Interface().(error)
	}
	if err != nil || len(out) == 1 {
		return nil, err
	}
	res, encErr := wire.Encode(out[0].Interface())
	if encErr != nil {
		return nil, fmt.Errorf("encode workflow result: %w", encErr)
	}
	return res, nil
}

// closeCommand maps the root function's outcome to the command that closes the run.
func closeCommand(e *env, result *wire.Payload, err error) wire.Command {
	if err == nil {
		return wire.Command{Type: wire.CompleteWorkflow, Result: result}
	}
	if errors.Is(err, ErrCanceled) && e.canceled {
		return wire.Command{Type: wire.CancelWorkflow}
	}
	var ae *ActivityError
	if errors.As(err, &ae) {
		f := ae.Failure
		if ae.TimedOut && f.Type == "" {
			f = wire.Failure{Type: "ActivityTimeout", Message: ae.Error()}
		}
		return wire.Command{Type: wire.FailWorkflow, Failure: &f}
	}
	return wire.Command{Type: wire.FailWorkflow, Failure: &wire.Failure{Type: "WorkflowError", Message: err.Error()}}
}
