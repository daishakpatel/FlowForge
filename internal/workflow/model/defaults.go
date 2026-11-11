package model

// Defaults applied by Decode to fields that are absent from the JSON. An explicit value, even
// zero, is kept so that dag.Validate can reject it (for example "max_attempts": 0).
const (
	DefaultOnFailure          = OnFailureFail
	DefaultWorkflowTimeout    = 0 // seconds; no workflow-level timeout
	DefaultMaxParallelTasks   = 0 // no per-run limit
	DefaultQueue              = "default"
	DefaultTaskTimeoutSeconds = 3600
	DefaultRunIf              = RunIfOnSuccess
	DefaultMaxAttempts        = 1
	DefaultStrategy           = StrategyFixed
	DefaultInitialDelayMS     = 1000
	DefaultMaxDelayMS         = 60000
	DefaultMultiplier         = 2.0
	DefaultJitter             = 0.0
)

// MaxDefinitionBytes is the largest definition Decode accepts (FR-SEC-05: ≤ 512 KiB).
const MaxDefinitionBytes = 512 << 10
