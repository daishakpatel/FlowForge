package fsm_test

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"strings"
	"testing"
)

// TestNoMapIterationOutsideSortedKeys enforces the determinism rule that FSM output never depends
// on Go's random map iteration order (00C §8.1): the only range over a map in the package's
// non-test code is inside sortedKeys, which every other iteration goes through.
func TestNoMapIterationOutsideSortedKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("static source check (about 10 s to type-check); runs in the full suite")
	}
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	if _, err := conf.Check("fsm", fset, files, info); err != nil {
		t.Fatalf("type-check: %v", err)
	}
	helper := false
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				r, ok := n.(*ast.RangeStmt)
				if !ok {
					return true
				}
				if _, isMap := info.TypeOf(r.X).Underlying().(*types.Map); isMap {
					if fn.Name.Name == "sortedKeys" {
						helper = true
					} else {
						t.Errorf("%s: %s ranges over a map; use sortedKeys", fset.Position(r.Pos()), fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	if !helper {
		t.Error("sortedKeys not found: the test checks nothing")
	}
}
