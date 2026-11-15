package expr

import (
	"errors"
	"fmt"

	"github.com/daishakpatel/flowforge/internal/workflow/template"
)

// node is a parsed expression: lit, path, not or binary.
type node interface{}

type lit struct{ v any } // float64, string, bool or nil

type path struct {
	text string
	ref  template.Ref
}

type not struct{ x node }

type binary struct {
	op   string
	l, r node
}

// Binding powers (Pratt): || < && < comparisons. "!" takes a comparison as its operand.
const (
	bpOr  = 1
	bpAnd = 2
	bpCmp = 3
)

func infixBP(op string) int {
	switch op {
	case "||":
		return bpOr
	case "&&":
		return bpAnd
	case "==", "!=", "<", "<=", ">", ">=":
		return bpCmp
	}
	return 0
}

// Expr is a parsed expression, safe for concurrent use.
type Expr struct {
	root node
}

// Parse parses s; the error says what is wrong and where.
func Parse(s string) (*Expr, error) {
	toks, err := lex(s)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	root, err := p.parse(0, 0)
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tEOF {
		return nil, fmt.Errorf("at %d: unexpected %q", t.pos, t.text)
	}
	return &Expr{root: root}, nil
}

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tEOF {
		p.pos++
	}
	return t
}

// parse parses operators binding tighter than minBP, left to right.
func (p *parser) parse(minBP, depth int) (node, error) {
	if depth > maxDepth {
		return nil, fmt.Errorf("expression nests deeper than %d", maxDepth)
	}
	left, err := p.prefix(depth)
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		bp := infixBP(t.text)
		if t.kind != tOp || bp == 0 || bp <= minBP {
			return left, nil
		}
		p.next()
		right, err := p.parse(bp, depth+1)
		if err != nil {
			return nil, err
		}
		if bp == bpCmp && infixBP(p.peek().text) == bpCmp && p.peek().kind == tOp {
			return nil, fmt.Errorf("at %d: comparisons do not chain; use && and parentheses", p.peek().pos)
		}
		left = binary{op: t.text, l: left, r: right}
	}
}

func (p *parser) prefix(depth int) (node, error) {
	t := p.next()
	switch t.kind {
	case tNum:
		return lit{t.num}, nil
	case tStr:
		return lit{t.text}, nil
	case tIdent:
		return ident(t)
	case tLParen:
		x, err := p.parse(0, depth+1)
		if err != nil {
			return nil, err
		}
		if c := p.next(); c.kind != tRParen {
			return nil, fmt.Errorf("at %d: expected ')'", c.pos)
		}
		return x, nil
	case tOp:
		if t.text == "!" {
			x, err := p.parse(bpAnd, depth+1)
			return not{x}, err
		}
	case tEOF:
		return nil, errors.New("unexpected end of expression")
	}
	return nil, fmt.Errorf("at %d: unexpected %q", t.pos, t.text)
}

func ident(t token) (node, error) {
	switch t.text {
	case "true":
		return lit{true}, nil
	case "false":
		return lit{false}, nil
	case "null":
		return lit{nil}, nil
	}
	ref, err := template.ParseRef(t.text)
	if err != nil {
		return nil, fmt.Errorf("at %d: %w", t.pos, err)
	}
	return path{text: t.text, ref: ref}, nil
}

// Refs returns the task ids the expression refers to, once each, in order of appearance.
func (x *Expr) Refs() []string {
	var out []string
	seen := map[string]bool{}
	var visit func(n node)
	visit = func(n node) {
		switch n := n.(type) {
		case path:
			if n.ref.Task != "" && !seen[n.ref.Task] {
				seen[n.ref.Task] = true
				out = append(out, n.ref.Task)
			}
		case not:
			visit(n.x)
		case binary:
			visit(n.l)
			visit(n.r)
		}
	}
	visit(x.root)
	return out
}
