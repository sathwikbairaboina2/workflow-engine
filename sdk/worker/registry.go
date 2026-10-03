package worker

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/internal/wfrt"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

var (
	ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errType = reflect.TypeOf((*error)(nil)).Elem()
)

// activityFn is a validated activity: func(ctx[, in]) error or func(ctx[, in]) (R, error).
type activityFn struct {
	fn      reflect.Value
	inType  reflect.Type // nil when the function takes no input
	hasRes  bool
	display string
}

func newActivityFn(fn any) (activityFn, error) {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func {
		return activityFn{}, fmt.Errorf("activity must be a function, got %T", fn)
	}
	if t.NumIn() < 1 || t.NumIn() > 2 || t.In(0) != ctxType {
		return activityFn{}, fmt.Errorf("activity %s must take (context.Context[, input])", t)
	}
	a := activityFn{fn: reflect.ValueOf(fn), display: t.String()}
	if t.NumIn() == 2 {
		a.inType = t.In(1)
	}
	switch {
	case t.NumOut() == 1 && t.Out(0) == errType:
	case t.NumOut() == 2 && t.Out(1) == errType:
		a.hasRes = true
	default:
		return activityFn{}, fmt.Errorf("activity %s must return error or (result, error)", t)
	}
	return a, nil
}

// call decodes input, invokes the activity and encodes its result.
func (a activityFn) call(ctx context.Context, input *wire.Payload) (*wire.Payload, error) {
	args := []reflect.Value{reflect.ValueOf(ctx)}
	if a.inType != nil {
		in := reflect.New(a.inType)
		if err := input.Decode(in.Interface()); err != nil {
			return nil, fmt.Errorf("decode activity input: %w", err)
		}
		args = append(args, in.Elem())
	}
	out := a.fn.Call(args)
	if e := out[len(out)-1]; !e.IsNil() {
		return nil, e.Interface().(error)
	}
	if !a.hasRes {
		return nil, nil
	}
	return wire.Encode(out[0].Interface())
}

type registry struct {
	mu         sync.RWMutex
	workflows  map[string]any
	activities map[string]activityFn
}

func newRegistry() *registry {
	return &registry{workflows: map[string]any{}, activities: map[string]activityFn{}}
}

func (r *registry) addWorkflow(name string, fn any) {
	if err := wfrt.ValidateWorkflowFunc(fn); err != nil {
		panic("worker: " + err.Error())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workflows[name] = fn
}

func (r *registry) addActivity(name string, fn any) {
	a, err := newActivityFn(fn)
	if err != nil {
		panic("worker: " + err.Error())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.activities[name] = a
}

func (r *registry) workflow(name string) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.workflows[name]
	return fn, ok
}

func (r *registry) activity(name string) (activityFn, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.activities[name]
	return a, ok
}
