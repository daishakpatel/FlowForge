// Package service is the API's application layer (task 02.04): the use cases handlers call, free
// of HTTP and SQL, so other entry points (cron triggers, approved AI actions) reuse them.
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/dag"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// WorkflowService creates and reads workflow definitions (FR-WF-01).
type WorkflowService struct {
	DB      persistence.Querier
	DefRepo persistence.DefinitionRepo
}

// Create decodes raw strictly, validates the DAG and stores the definition. Definitions are
// immutable: a duplicate (name, version) is CONFLICT. An omitted version becomes one above the
// name's latest (1 for a new name); two concurrent creates of the same name without a version
// can therefore race, and the loser gets CONFLICT. What is stored is the normalized definition,
// with every default applied, so runs never depend on defaults changing later.
//
// Errors: PAYLOAD_TOO_LARGE above 512 KiB, INVALID_ARGUMENT for malformed JSON or unknown
// fields, INVALID_WORKFLOW with one detail per issue (code, task_id, message).
func (s *WorkflowService) Create(ctx context.Context, raw []byte, actor string) (persistence.DefinitionView, error) {
	def, err := model.Decode(raw)
	if err != nil {
		return persistence.DefinitionView{}, err
	}
	if !hasVersion(raw) {
		if def.Version, err = s.nextVersion(ctx, def.Name); err != nil {
			return persistence.DefinitionView{}, err
		}
	}
	if issues := dag.Validate(def); len(issues) > 0 {
		details := make([]apperrors.Detail, len(issues))
		for i, is := range issues {
			details[i] = apperrors.Detail{Code: string(is.Code), TaskID: is.TaskID, Message: is.Message}
		}
		return persistence.DefinitionView{}, apperrors.InvalidWorkflow("workflow definition is invalid", details...)
	}
	normalized, err := json.Marshal(def)
	if err != nil {
		return persistence.DefinitionView{}, fmt.Errorf("encode definition: %w", err)
	}
	return s.DefRepo.Create(ctx, s.DB, persistence.NewDefinition{
		Name: def.Name, Version: def.Version, Definition: normalized, CreatedBy: actor,
	})
}

// hasVersion reports whether the top-level object has a "version" key (model.Decode cannot
// tell an absent version from 0). raw has already been decoded, so it is a valid object.
func hasVersion(raw []byte) bool {
	var probe map[string]json.RawMessage
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&probe); err != nil {
		return true // let validation report it
	}
	_, ok := probe["version"]
	return ok
}

func (s *WorkflowService) nextVersion(ctx context.Context, name string) (int, error) {
	latest, err := s.DefRepo.Latest(ctx, s.DB, name)
	switch {
	case apperrors.CodeOf(err) == apperrors.CodeNotFound:
		return 1, nil
	case err != nil:
		return 0, err
	}
	return latest.Version + 1, nil
}

// Get returns a definition by id, or NOT_FOUND.
func (s *WorkflowService) Get(ctx context.Context, id string) (persistence.DefinitionView, error) {
	return s.DefRepo.Get(ctx, s.DB, id)
}

// GetByName returns version of name, or its latest version when version is 0.
func (s *WorkflowService) GetByName(ctx context.Context, name string, version int) (persistence.DefinitionView, error) {
	if version == 0 {
		return s.DefRepo.Latest(ctx, s.DB, name)
	}
	return s.DefRepo.GetByNameVersion(ctx, s.DB, name, version)
}

// List returns one page of definitions, newest first, and the next cursor ("" at the end).
func (s *WorkflowService) List(ctx context.Context, page persistence.Page) ([]persistence.DefinitionView, string, error) {
	return s.DefRepo.List(ctx, s.DB, page)
}
