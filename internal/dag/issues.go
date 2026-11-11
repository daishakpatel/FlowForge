// Package dag validates workflow definitions and answers graph questions about them (task 02.02,
// FR-WF-02). It is pure: no database, transport or clock (ADR-020, lint-enforced).
package dag

import (
	"cmp"
	"fmt"
	"slices"
)

// IssueCode identifies one kind of validation problem. The values are stable: the API returns
// them in INVALID_WORKFLOW details (02.04), so never rename one.
type IssueCode string

// The validation issue codes (task 02.02 §5).
const (
	CodeDuplicateTaskID      IssueCode = "DUPLICATE_TASK_ID"
	CodeInvalidTaskID        IssueCode = "INVALID_TASK_ID"
	CodeUnknownDependency    IssueCode = "UNKNOWN_DEPENDENCY"
	CodeSelfDependency       IssueCode = "SELF_DEPENDENCY"
	CodeCycle                IssueCode = "CYCLE"
	CodeTooManyTasks         IssueCode = "TOO_MANY_TASKS"
	CodeInvalidRetry         IssueCode = "INVALID_RETRY"
	CodeInvalidTimeout       IssueCode = "INVALID_TIMEOUT"
	CodeMissingType          IssueCode = "MISSING_TYPE"
	CodeInvalidQueue         IssueCode = "INVALID_QUEUE"
	CodeInvalidRunIf         IssueCode = "INVALID_RUN_IF"
	CodeEmptyExpression      IssueCode = "EMPTY_EXPRESSION"
	CodeInvalidWorkflowField IssueCode = "INVALID_WORKFLOW_FIELD"
)

// IssueCodes lists every issue code.
func IssueCodes() []IssueCode {
	return []IssueCode{CodeDuplicateTaskID, CodeInvalidTaskID, CodeUnknownDependency, CodeSelfDependency,
		CodeCycle, CodeTooManyTasks, CodeInvalidRetry, CodeInvalidTimeout, CodeMissingType, CodeInvalidQueue,
		CodeInvalidRunIf, CodeEmptyExpression, CodeInvalidWorkflowField}
}

// ValidationIssue is one problem in a definition. TaskID is empty for workflow-level issues.
// Path is set only for CYCLE: each task depends on the next, and the last equals the first.
type ValidationIssue struct {
	Code    IssueCode `json:"code"`
	TaskID  string    `json:"task_id,omitempty"`
	Message string    `json:"message"`
	Path    []string  `json:"path,omitempty"`
}

// collector accumulates issues; sorted returns them in a deterministic order.
type collector []ValidationIssue

func (c *collector) add(code IssueCode, taskID, format string, args ...any) {
	*c = append(*c, ValidationIssue{Code: code, TaskID: taskID, Message: fmt.Sprintf(format, args...)})
}

// sorted orders issues by (task id, code, message) and drops exact duplicates, which a repeated
// task ID would otherwise produce.
func (c collector) sorted() []ValidationIssue {
	out := slices.Clone(c)
	slices.SortStableFunc(out, func(a, b ValidationIssue) int {
		return cmp.Or(cmp.Compare(a.TaskID, b.TaskID), cmp.Compare(a.Code, b.Code), cmp.Compare(a.Message, b.Message))
	})
	return slices.CompactFunc(out, func(a, b ValidationIssue) bool {
		return a.TaskID == b.TaskID && a.Code == b.Code && a.Message == b.Message
	})
}
