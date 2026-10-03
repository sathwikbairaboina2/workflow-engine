// Package naming derives the registered name of a workflow or activity function.
package naming

import (
	"reflect"
	"runtime"
	"strings"
)

// Name returns fn when it is a string, otherwise the bare function name: the part after the last dot,
// without the "-fm" suffix the compiler adds to method values.
func Name(fn any) string {
	if s, ok := fn.(string); ok {
		return s
	}
	full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	if i := strings.LastIndex(full, "."); i >= 0 {
		full = full[i+1:]
	}
	return strings.TrimSuffix(full, "-fm")
}
