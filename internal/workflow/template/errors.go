package template

import (
	"errors"
	"fmt"
)

// TemplateError is a placeholder that cannot be resolved: bad syntax, a task outside the scope
// (not an ancestor), or a path that does not exist. Path is the placeholder's text.
//
//nolint:revive // the task contract names it TemplateError (03.06 §4)
type TemplateError struct {
	Path   string
	Reason string
}

func (e *TemplateError) Error() string {
	return fmt.Sprintf("template ${%s}: %s", e.Path, e.Reason)
}

// ErrTooLarge is wrapped by the error Resolve returns when the resolved input is over
// MaxInputBytes.
var ErrTooLarge = errors.New("resolved input is too large")
