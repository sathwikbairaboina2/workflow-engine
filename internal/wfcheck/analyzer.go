// Package wfcheck is a static analyzer that flags nondeterministic constructs inside workflow code.
// A workflow function is any function or function literal whose first parameter is workflow.Context.
package wfcheck

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer reports time, math/rand, go statements and select statements in workflow functions.
var Analyzer = &analysis.Analyzer{
	Name:     "wfcheck",
	Doc:      "report nondeterministic code in workflow functions (time.Now, time.Sleep, math/rand, go, select)",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// timeFuncs maps flagged functions of package time to the replacement named in the message.
var timeFuncs = map[string]string{
	"Now":       "workflow.Now",
	"Since":     "workflow.Now",
	"Until":     "workflow.Now",
	"Sleep":     "workflow.Sleep",
	"After":     "workflow.NewTimer",
	"Tick":      "workflow.NewTimer",
	"NewTimer":  "workflow.NewTimer",
	"NewTicker": "workflow.NewTimer",
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	reported := map[token.Pos]bool{} // nested workflow functions are visited twice
	report := func(pos token.Pos, format string, args ...any) {
		if !reported[pos] {
			reported[pos] = true
			pass.Reportf(pos, format, args...)
		}
	}
	insp.Preorder([]ast.Node{(*ast.FuncDecl)(nil), (*ast.FuncLit)(nil)}, func(n ast.Node) {
		var typ *ast.FuncType
		var body *ast.BlockStmt
		switch f := n.(type) {
		case *ast.FuncDecl:
			typ, body = f.Type, f.Body
		case *ast.FuncLit:
			typ, body = f.Type, f.Body
		}
		if body == nil || !firstParamIsWorkflowContext(pass, typ) {
			return
		}
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.GoStmt:
				report(x.Pos(), "go statement in workflow code; use workflow.Go")
			case *ast.SelectStmt:
				report(x.Pos(), "select statement in workflow code; use workflow.NewSelector")
			case *ast.CallExpr:
				checkCall(pass, x, report)
			}
			return true
		})
	})
	return nil, nil
}

func checkCall(pass *analysis.Pass, call *ast.CallExpr, report func(token.Pos, string, ...any)) {
	var id *ast.Ident
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		id = fun.Sel
	case *ast.Ident:
		id = fun
	default:
		return
	}
	fn, ok := pass.TypesInfo.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return
	}
	switch path := fn.Pkg().Path(); path {
	case "time":
		// Methods such as Time.Add live in package time too; only the package-level functions are flagged.
		if sig, _ := fn.Type().(*types.Signature); sig != nil && sig.Recv() != nil {
			return
		}
		if use, bad := timeFuncs[fn.Name()]; bad {
			report(call.Pos(), "time.%s is not deterministic in workflow code; use %s", fn.Name(), use)
		}
	case "math/rand", "math/rand/v2":
		report(call.Pos(), "%s is not deterministic in workflow code; use workflow.SideEffect", path)
	}
}

func firstParamIsWorkflowContext(pass *analysis.Pass, typ *ast.FuncType) bool {
	if typ.Params == nil || len(typ.Params.List) == 0 {
		return false
	}
	return isWorkflowContext(pass.TypesInfo.TypeOf(typ.Params.List[0].Type))
}

// isWorkflowContext accepts workflow.Context (also through an alias, as the SDK facade declares it) and a
// pointer to it, whether it names the facade package or the runtime package behind it.
func isWorkflowContext(t types.Type) bool {
	if t == nil {
		return false
	}
	if a, ok := t.(*types.Alias); ok {
		if obj := a.Obj(); obj.Name() == "Context" && obj.Pkg() != nil && strings.HasSuffix(obj.Pkg().Path(), "/sdk/workflow") {
			return true
		}
	}
	t = types.Unalias(t)
	if p, ok := t.(*types.Pointer); ok {
		t = types.Unalias(p.Elem())
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Name() != "Context" || named.Obj().Pkg() == nil {
		return false
	}
	path := named.Obj().Pkg().Path()
	return strings.HasSuffix(path, "/sdk/workflow") || strings.HasSuffix(path, "/sdk/internal/wfrt")
}
