// Package expr is the run_if_expr language (task 03.06, 00E §3.4): comparisons and boolean logic
// over literals and paths into the run input and ancestor tasks. It is pure (ADR-020).
//
// Grammar (EBNF):
//
//	expr    = or
//	or      = and { "||" and }
//	and     = not { "&&" not }
//	not     = [ "!" ] cmp
//	cmp     = primary [ ( "==" | "!=" | "<" | "<=" | ">" | ">=" ) primary ]
//	primary = literal | path | "(" expr ")"
//	literal = number | string | "true" | "false" | "null"
//	path    = run.input<keys> | tasks.<id>.status | tasks.<id>.result<keys>
//
// "!" negates a whole comparison: !a == b means !(a == b). Comparisons do not chain. Strings use
// double quotes (Go escapes) or single quotes (no escapes). Keys are dotted with [n] indexes, as
// in templates.
package expr

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Limits on an expression.
const (
	MaxLength = 4096 // bytes
	maxDepth  = 64   // nesting of parentheses and "!"
)

type tokKind int

const (
	tEOF tokKind = iota
	tNum
	tStr
	tIdent // a path or a keyword
	tOp
	tLParen
	tRParen
)

type token struct {
	kind tokKind
	text string // tStr: the unquoted value
	num  float64
	pos  int
}

var operators = []string{"==", "!=", "<=", ">=", "&&", "||", "<", ">", "!"}

// lex splits s into tokens, ending with tEOF.
func lex(s string) ([]token, error) {
	if len(s) > MaxLength {
		return nil, fmt.Errorf("expression is %d bytes; the limit is %d", len(s), MaxLength)
	}
	var out []token
	for i := 0; i < len(s); {
		t, n, err := lexOne(s[i:])
		if err != nil {
			return nil, fmt.Errorf("at %d: %w", i, err)
		}
		if t.kind != tEOF { // whitespace
			t.pos = i
			out = append(out, t)
		}
		i += n
	}
	return append(out, token{kind: tEOF, pos: len(s)}), nil
}

// lexOne reads the token at the start of s and its length; whitespace is a tEOF token.
func lexOne(s string) (token, int, error) {
	c := s[0]
	switch {
	case c == ' ' || c == '\t' || c == '\n' || c == '\r':
		return token{}, 1, nil
	case c == '(':
		return token{kind: tLParen, text: "("}, 1, nil
	case c == ')':
		return token{kind: tRParen, text: ")"}, 1, nil
	case c == '"' || c == '\'':
		return lexString(s)
	case isDigit(c) || (c == '-' && len(s) > 1 && isDigit(s[1])):
		return lexNumber(s)
	case isLetter(c):
		n := 1
		for n < len(s) && isPathChar(s[n]) {
			n++
		}
		return token{kind: tIdent, text: s[:n]}, n, nil
	}
	for _, o := range operators {
		if strings.HasPrefix(s, o) {
			return token{kind: tOp, text: o}, len(o), nil
		}
	}
	return token{}, 0, fmt.Errorf("unexpected %q", c)
}

// lexString reads a quoted string at the start of s.
func lexString(s string) (token, int, error) {
	q := s[0]
	for i := 1; i < len(s); i++ {
		switch {
		case s[i] == '\\' && q == '"':
			i++
		case s[i] == q:
			if q == '\'' {
				return token{kind: tStr, text: s[1:i]}, i + 1, nil
			}
			v, err := strconv.Unquote(s[:i+1])
			if err != nil {
				return token{}, 0, fmt.Errorf("bad string %s: %w", s[:i+1], err)
			}
			return token{kind: tStr, text: v}, i + 1, nil
		}
	}
	return token{}, 0, errors.New("unterminated string")
}

// lexNumber reads -?digits[.digits][e[+-]digits] at the start of s.
func lexNumber(s string) (token, int, error) {
	n := 0
	if s[0] == '-' {
		n++
	}
	digits := func() {
		for n < len(s) && isDigit(s[n]) {
			n++
		}
	}
	digits()
	if n < len(s) && s[n] == '.' {
		n++
		digits()
	}
	if n < len(s) && (s[n] == 'e' || s[n] == 'E') {
		n++
		if n < len(s) && (s[n] == '+' || s[n] == '-') {
			n++
		}
		digits()
	}
	v, err := strconv.ParseFloat(s[:n], 64)
	if err != nil || math.IsInf(v, 0) {
		return token{}, 0, fmt.Errorf("bad number %q", s[:n])
	}
	return token{kind: tNum, text: s[:n], num: v}, n, nil
}

func isDigit(c byte) bool  { return c >= '0' && c <= '9' }
func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
func isPathChar(c byte) bool {
	return isLetter(c) || isDigit(c) || c == '_' || c == '-' || c == '.' || c == '[' || c == ']'
}
