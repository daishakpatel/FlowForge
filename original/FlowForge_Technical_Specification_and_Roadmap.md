# FlowForge --- Distributed Workflow Runtime & AI Operations Platform

> **Project specification, architecture, implementation roadmap, and
> engineering decision record**\
> Version: 1.0\
> Date: November 2, 2026\
> Primary language: Go\
> Supporting language: Python\
> Intended use: Portfolio-grade distributed systems project
> demonstrating backend, platform, streaming, reliability, and
> applied-AI engineering.

------------------------------------------------------------------------

## Table of contents

1.  [Executive summary](#1-executive-summary)
2.  [Project positioning and goals](#2-project-positioning-and-goals)
3.  [What FlowForge is---and is not](#3-what-flowforge-isand-is-not)
4.  [Core use case and end-to-end
    scenario](#4-core-use-case-and-end-to-end-scenario)
5.  [System architecture](#5-system-architecture)
6.  [Architecture decisions and alternative
    approaches](#6-architecture-decisions-and-alternative-approaches)
7.  [Detailed component scope](#7-detailed-component-scope)
8.  [Raft-backed replicated key-value
    store](#8-raft-backed-replicated-key-value-store)
9.  [Workflow orchestration engine](#9-workflow-orchestration-engine)
10. [Kafka streaming and market-data
    pipeline](#10-kafka-streaming-and-market-data-pipeline)
11. [AI Operations Copilot: RAG, LangChain, and
    LangGraph](#11-ai-operations-copilot-rag-langchain-and-langgraph)
12. [Data model and event contracts](#12-data-model-and-event-contracts)
13. [API design](#13-api-design)
14. [Repository structure](#14-repository-structure)
15. [Local development environment](#15-local-development-environment)
16. [Testing and fault injection](#16-testing-and-fault-injection)
17. [Observability and operations](#17-observability-and-operations)
18. [Security and reliability
    requirements](#18-security-and-reliability-requirements)
19. [Implementation phases and
    milestones](#19-implementation-phases-and-milestones)
20. [Scope boundaries and stretch
    goals](#20-scope-boundaries-and-stretch-goals)
21. [Portfolio demonstration and
    documentation](#21-portfolio-demonstration-and-documentation)
22. [Resume positioning](#22-resume-positioning)
23. [Risks and mitigations](#23-risks-and-mitigations)
24. [Definition of done](#24-definition-of-done)
25. [Recommended build order](#25-recommended-build-order)
26. [Glossary](#26-glossary)

------------------------------------------------------------------------

# 1. Executive summary

**FlowForge** is a distributed workflow runtime and AI operations
platform. It coordinates durable workflows across distributed workers,
uses a Raft-backed replicated key-value store for cluster coordination,
processes streaming market data through Apache Kafka, and provides an AI
operations copilot that can investigate failures and recommend recovery
actions.

The project is intentionally designed as one coherent platform rather
than several unrelated portfolio projects.

The system has three primary planes:

1.  **Control plane --- Go:** accepts workflow definitions, schedules
    DAG tasks, tracks durable execution state, coordinates workers, and
    recovers work after failures.
2.  **Data plane --- Go + Kafka:** runs distributed workers and stream
    processors, transports events through Kafka, and stores time-series
    market aggregates in TimescaleDB.
3.  **AI plane --- Python:** uses retrieval-augmented generation (RAG),
    LangChain integrations, and LangGraph stateful agent workflows to
    investigate incidents and recommend actions.

A Raft-backed replicated key-value store supports control-plane
coordination, including scheduler leadership, worker registration,
leases, and cluster metadata.

The market-data pipeline is the main workload used to demonstrate the
runtime. FlowForge schedules ingestion and processing jobs, manages
retries and recovery, tracks consumer lag, and can replay data after a
failure. The AI copilot can then investigate why a pipeline failed or
slowed down by combining workflow history, logs, metrics, and runbook
knowledge.

## Recommended technology choices

  ---------------------------------------------------------------------------
  Area                    Recommended choice          Purpose
  ----------------------- --------------------------- -----------------------
  Core platform           Go                          Workflow API,
                                                      scheduler, workers,
                                                      coordination

  Internal service        gRPC + Protocol Buffers     Typed
  communication                                       service-to-service
                                                      contracts

  External API            REST/JSON                   Easy integration and
                                                      demonstration

  Consensus               `go.etcd.io/etcd/raft/v3`   Raft consensus
                                                      implementation

  Coordination state      Custom Go state machine     FlowForge-specific
                                                      replicated KV semantics

  Workflow persistence    PostgreSQL                  Durable workflow
                                                      definitions, runs, task
                                                      history

  Event transport         Apache Kafka                Durable event streams
                                                      and replay

  Stream processor        Go                          Validation,
                                                      deduplication, windowed
                                                      aggregates

  Time-series storage     TimescaleDB                 OHLCV and other
                                                      market-data aggregates

  AI service              Python + FastAPI            AI investigation API
                                                      and tools

  Agent orchestration     LangGraph                   Stateful investigation
                                                      flow and human approval

  AI integrations         LangChain                   Model, embedding,
                                                      retriever, and tool
                                                      integrations

  Vector search           PostgreSQL + pgvector       Retrieval over runbooks
                                                      and technical
                                                      documentation

  Frontend                React + TypeScript          Workflow and pipeline
                                                      operations dashboard

  Observability           OpenTelemetry, Prometheus,  Traces, metrics,
                          Grafana                     dashboards

  Local infrastructure    Docker Compose              Repeatable
                                                      multi-service
                                                      development

  CI                      GitHub Actions              Automated checks and
                                                      test execution

  Later deployment        Kubernetes                  Optional
                                                      production-style
                                                      deployment
  ---------------------------------------------------------------------------

## High-level architecture

``` mermaid
flowchart TB
    User["User / Operator"] --> UI["React + TypeScript Dashboard"]
    UI --> API["Go Workflow API"]

    subgraph Control["CONTROL PLANE — GO"]
        API --> Scheduler["Scheduler / DAG Runtime"]
        Scheduler --> History["PostgreSQL Workflow History"]
        Scheduler --> Raft["Raft Coordination Cluster"]
        Scheduler --> WorkerRegistry["Worker Registry / Leases"]
    end

    subgraph Data["DATA PLANE"]
        Scheduler --> Queue["Task Dispatch"]
        Queue --> Workers["Distributed Go Workers"]
        Workers --> Kafka["Apache Kafka"]
        Kafka --> Processor["Go Stream Processor"]
        Processor --> TSDB["TimescaleDB"]
        Workers --> History
    end

    subgraph AI["AI PLANE — PYTHON"]
        AIAPI["FastAPI AI Service"] --> Agent["LangGraph Investigation Graph"]
        Agent --> RAG["LangChain Retrieval"]
        RAG --> Vector["PostgreSQL + pgvector"]
        Agent --> Tools["Workflow / Logs / Metrics Tools"]
    end

    History --> Tools
    Kafka --> Tools
    Processor --> Tools
    UI --> AIAPI

    OTel["OpenTelemetry"] -.-> API
    OTel -.-> Scheduler
    OTel -.-> Workers
    OTel -.-> Processor
    Prom["Prometheus"] -.-> OTel
    Grafana["Grafana"] --> Prom
```

The diagram describes the intended logical architecture. The first
implementation should be smaller: build the workflow runtime and a
single worker path before adding consensus, Kafka, market data, and AI.

------------------------------------------------------------------------

# 2. Project positioning and goals

## 2.1 Portfolio positioning

The project should communicate that its author can build and reason
about backend and platform systems---not only UI features or isolated
CRUD APIs.

The target engineering themes are:

-   Distributed systems and coordination
-   Durable workflow execution
-   Backend API and service design
-   Concurrency and worker scheduling
-   Fault tolerance and recovery
-   Event-driven architecture
-   Streaming data processing
-   Database design and consistency
-   Observability and operational debugging
-   Applied AI with retrieval and tool use
-   Testing under failures and load

The strongest positioning is:

> **A distributed workflow runtime that coordinates fault-tolerant
> workers, processes replayable event streams, and uses an AI operations
> agent to investigate system incidents.**

This is more cohesive than presenting a Raft implementation, a
stock-data pipeline, a chatbot, and a workflow engine as four separate
projects.

## 2.2 Main objectives

1.  Build a functioning workflow engine that executes directed acyclic
    graphs (DAGs).
2.  Persist workflow and task state so executions can recover after
    process restarts.
3.  Coordinate scheduler leadership and worker ownership using a
    replicated Raft-backed KV store.
4.  Run distributed workers that can claim, execute, retry, and report
    tasks.
5.  Integrate Kafka for event transport and replay.
6.  Build a market-data workload that demonstrates real streaming
    requirements.
7.  Add an AI operations copilot that investigates failures using
    structured tools and RAG.
8.  Demonstrate reliability through integration tests, fault injection,
    load tests, and observable behavior.
9.  Document the system so a reviewer can understand the design,
    tradeoffs, and evidence.

## 2.3 Non-goals for the first version

Do not attempt to build all of the following immediately:

-   A full Temporal or Airflow replacement
-   A complete Kafka replacement
-   A production-grade consensus library from scratch
-   A high-frequency trading system
-   A full financial data vendor or real-time exchange feed
-   A general-purpose autonomous production agent
-   A Kubernetes operator
-   A large microservices estate before the core workflow path works

The goal is a rigorous, demonstrable portfolio system---not a claim that
the first release is production-ready for critical workloads.

## 2.4 Success criteria

A successful first substantial release should demonstrate:

-   A workflow with multiple dependent tasks can execute across workers.
-   Workflow and task state survives an API or worker restart.
-   A failed task can retry according to policy.
-   A worker that disappears can have its lease expire and its work
    safely reassigned.
-   A scheduler leader can fail and another node can take over when a
    Raft quorum remains.
-   Kafka events can be consumed, processed, persisted, and replayed.
-   The market-data pipeline can handle duplicate, late, and
    out-of-order events according to documented rules.
-   An AI investigation can retrieve relevant runbooks and inspect real
    workflow/metric data.
-   The system exposes useful traces, metrics, logs, and dashboards.
-   Tests reproduce important failure scenarios.

------------------------------------------------------------------------

# 3. What FlowForge is---and is not

## 3.1 FlowForge is the top-level orchestrator

FlowForge owns the lifecycle of a workflow:

-   Workflow submission
-   DAG validation
-   Scheduling
-   Task dispatch
-   Worker assignment
-   Retry and timeout policies
-   Durable execution history
-   Recovery after failure
-   Workflow-level observability
-   Human approval gates for selected operations

A workflow is composed of tasks. Tasks are executed by workers. A task
may publish or consume Kafka events, write to a database, call another
service, or invoke an AI investigation.

## 3.2 Raft is a coordination primitive

Raft is not the workflow engine and is not the event bus.

The Raft-backed key-value store provides a strongly coordinated view of
selected cluster metadata, such as:

-   Which scheduler currently owns leadership
-   Which workers are registered
-   Which workers hold active leases
-   Which scheduler owns a partition or task-claiming responsibility
-   Cluster configuration and coordination metadata

It should not be used as the primary store for all workflow history or
all market events.

## 3.3 Kafka is the event transport

Kafka is used for durable, partitioned event streams:

-   Market tick events
-   Pipeline status events
-   Processing results
-   Notifications
-   Replayable input streams

Kafka is not the scheduler. It does not replace FlowForge's workflow
state machine.

## 3.4 LangGraph is the internal AI investigation orchestrator

LangGraph coordinates the steps inside an AI investigation:

-   Understand the incident question
-   Retrieve relevant runbooks and documentation
-   Query workflow state
-   Inspect logs and metrics
-   Form a diagnosis
-   Check whether evidence supports the diagnosis
-   Recommend a recovery action
-   Pause for human approval when appropriate

FlowForge remains the outer workflow orchestrator. It can schedule an
`AI_INVESTIGATION` task, invoke the Python AI service, persist the
result, and continue the parent workflow.

This avoids building two competing top-level workflow engines.

------------------------------------------------------------------------

# 4. Core use case and end-to-end scenario

## 4.1 Primary demonstration: market-data pipeline

The main demonstration should be a scheduled market-data workflow.

Example workflow:

1.  Start a scheduled run for a selected group of symbols.
2.  Fetch synthetic or historical market events.
3.  Validate and normalize the incoming events.
4.  Publish events to Kafka, partitioned by symbol.
5.  Run distributed Go consumers.
6.  Deduplicate events and calculate time-window aggregates.
7.  Persist aggregates in TimescaleDB.
8.  Validate output completeness and freshness.
9.  Emit workflow and pipeline metrics.
10. If a stage fails or becomes unhealthy, create an AI investigation
    task.
11. The AI copilot inspects execution history, logs, metrics, Kafka lag,
    and runbooks.
12. The copilot produces a diagnosis and recommends a safe recovery
    action.
13. A human approves any action that can cause operational changes.
14. FlowForge retries, replays, escalates, or marks the workflow
    complete.
15. The full history remains available for audit and debugging.

## 4.2 Example incident

Operator asks:

> Why did the market-data pipeline fail yesterday at 2:30 PM?

The AI copilot should not simply guess based on an LLM response. It
should gather evidence:

-   Find the workflow run around the specified time.
-   Identify the failed task and attempt.
-   Read the task error and correlated logs.
-   Check Kafka consumer lag and rebalance events.
-   Check processor latency and database write latency.
-   Retrieve the relevant runbook.
-   Compare observed facts with known failure patterns.
-   Return a concise root-cause hypothesis with evidence and confidence.
-   Recommend a recovery action, such as replaying a time range or
    retrying a task.
-   Require approval before performing a disruptive action.

## 4.3 Example investigation sequence

``` mermaid
sequenceDiagram
    actor Operator
    participant UI as Dashboard
    participant FF as FlowForge API
    participant DB as Workflow DB
    participant AI as AI Service
    participant LG as LangGraph Agent
    participant Obs as Logs / Metrics
    participant RAG as Runbook Retrieval

    Operator->>UI: Ask why a pipeline failed
    UI->>FF: Request investigation
    FF->>DB: Load workflow run and task history
    FF->>AI: Create AI_INVESTIGATION task
    AI->>LG: Start investigation graph
    LG->>Obs: Query logs, metrics, consumer lag
    LG->>RAG: Retrieve relevant runbooks
    Obs-->>LG: Return evidence
    RAG-->>LG: Return supporting documents
    LG->>LG: Correlate evidence and form diagnosis
    LG-->>AI: Structured diagnosis and recommendation
    AI-->>FF: Investigation result
    FF->>DB: Persist report and evidence references
    FF-->>UI: Show result and approval request
    Operator->>UI: Approve or reject recovery
    UI->>FF: Submit approval decision
    FF->>DB: Record decision
    FF->>FF: Retry / replay / escalate
```

------------------------------------------------------------------------

# 5. System architecture

## 5.1 Three-plane architecture

### A. Control plane --- Go

Responsibilities:

-   Workflow API
-   Workflow definition validation
-   DAG dependency evaluation
-   Scheduler
-   Task dispatch
-   Worker registration and health
-   Lease management
-   Retry and timeout decisions
-   Workflow history persistence
-   Raft coordination integration
-   Recovery after process or worker failure

Suggested services/modules:

-   `api`
-   `workflow`
-   `scheduler`
-   `dispatcher`
-   `workerregistry`
-   `recovery`
-   `coordination`
-   `persistence`

### B. Data plane --- Go + Kafka

Responsibilities:

-   Worker execution
-   Kafka producer and consumer integration
-   Market-data ingestion
-   Stream processing
-   Validation and normalization
-   Deduplication
-   Window aggregation
-   Backpressure
-   Replay and backfill
-   Time-series persistence

Suggested services/modules:

-   `worker`
-   `marketdata-producer`
-   `stream-processor`
-   `kafka`
-   `timeseries`
-   `pipeline-monitor`

### C. AI plane --- Python

Responsibilities:

-   AI investigation API
-   Runbook and documentation ingestion
-   Chunking and embedding
-   Vector retrieval
-   LangGraph investigation graph
-   Workflow, logs, and metrics tool adapters
-   Evidence-based incident reports
-   Human approval handoff
-   Structured recommendations

Suggested services/modules:

-   `ai-api`
-   `ingestion`
-   `retrieval`
-   `agent`
-   `tools`
-   `schemas`

## 5.2 Component-level architecture

``` mermaid
flowchart LR
    subgraph Clients
        Dashboard["React Dashboard"]
        CLI["CLI / API Client"]
    end

    subgraph GoControl["Go Control Plane"]
        REST["REST API"]
        GRPC["gRPC Internal API"]
        DAG["DAG Validator"]
        SCHED["Scheduler"]
        DISPATCH["Task Dispatcher"]
        RECOVERY["Recovery Manager"]
        REG["Worker Registry"]
    end

    subgraph Persistence
        PG[("PostgreSQL")]
        RAFT[("3-node Raft KV")]
    end

    subgraph Workers["Go Worker Pool"]
        W1["Worker A"]
        W2["Worker B"]
        W3["Worker C"]
    end

    subgraph Streaming
        KAFKA[("Kafka")]
        PROC["Go Stream Processor"]
        TS[("TimescaleDB")]
    end

    subgraph AIService["Python AI Plane"]
        FAST["FastAPI"]
        GRAPH["LangGraph"]
        LC["LangChain Retrieval"]
        VDB[("pgvector")]
    end

    Dashboard --> REST
    CLI --> REST
    REST --> DAG
    REST --> PG
    DAG --> SCHED
    SCHED --> DISPATCH
    SCHED --> RECOVERY
    SCHED <--> RAFT
    SCHED --> REG
    DISPATCH --> W1
    DISPATCH --> W2
    DISPATCH --> W3
    W1 --> KAFKA
    W2 --> KAFKA
    W3 --> KAFKA
    KAFKA --> PROC
    PROC --> TS
    FAST --> GRAPH
    GRAPH --> LC
    LC --> VDB
    GRAPH --> PG
    REST <--> FAST
```

## 5.3 Deployment topology

For local development, use Docker Compose with multiple instances where
useful.

``` mermaid
flowchart TB
    subgraph Host["Local Development Host"]
        subgraph Compose["Docker Compose Network"]
            API["flowforge-api"]
            S1["scheduler-1"]
            S2["scheduler-2"]
            S3["scheduler-3"]
            R1["raft-1"]
            R2["raft-2"]
            R3["raft-3"]
            W1["worker-1"]
            W2["worker-2"]
            K["Kafka"]
            P["PostgreSQL / TimescaleDB"]
            RED["Redis (optional)"]
            A["AI FastAPI"]
            UI["Frontend"]
            OBS["Prometheus + Grafana + OTel"]
        end
    end

    API --> S1
    S1 <--> R1
    S2 <--> R2
    S3 <--> R3
    R1 <--> R2
    R2 <--> R3
    R1 <--> R3
    S1 --> W1
    S1 --> W2
    W1 --> K
    W2 --> K
    K --> P
    API --> P
    A --> P
    UI --> API
    OBS -.-> API
    OBS -.-> W1
    OBS -.-> W2
```

The first local version does not need every process shown here. Start
with one API, one scheduler, one worker, PostgreSQL, and then expand to
multiple nodes.

## 5.4 Logical boundaries and ownership

  -----------------------------------------------------------------------
  Component               Owns                    Does not own
  ----------------------- ----------------------- -----------------------
  Workflow API            External requests and   Consensus decisions
                          validation              

  Scheduler               DAG readiness and task  Market event storage
                          scheduling              

  Raft KV                 Replicated coordination Full workflow history
                          metadata                

  PostgreSQL              Durable workflow/task   High-volume event
                          records                 transport

  Workers                 Task execution and      Global scheduling
                          heartbeats              policy

  Kafka                   Durable event streams   Workflow DAG state
                          and offsets             

  Stream processor        Transformations and     Workflow-level retries
                          aggregation             

  TimescaleDB             Time-series aggregates  Worker lease state

  AI service              Investigation and       Top-level durable
                          recommendations         workflow scheduling

  LangGraph               Steps within an AI      Entire FlowForge
                          investigation           workflow lifecycle
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 6. Architecture decisions and alternative approaches

This section records the recommended choices and the main alternatives.
The implementation should document any later deviation.

## 6.1 Build Raft from scratch vs. use an existing Raft library

  -----------------------------------------------------------------------
  Approach          Advantages        Disadvantages     Recommendation
  ----------------- ----------------- ----------------- -----------------
  Implement Raft    Deep learning;    High correctness  Optional learning
  from scratch      full control;     risk; large       branch, not the
                    strong algorithm  testing burden;   main project path
                    exercise          easy to mishandle 
                                      elections, log    
                                      replication,      
                                      snapshots, and    
                                      membership        

  Use `etcd/raft`   Uses a well-known Must understand   **Recommended**
  and build the     consensus         the library's     
  state machine     implementation;   Ready/Advance     
                    lets the project  lifecycle and     
                    focus on          persistence       
                    persistence,      contract          
                    state-machine                       
                    semantics,                          
                    integration, and                    
                    failure behavior                    

  Use etcd as a     Fastest way to    Less ownership of Acceptable
  complete external get coordination  the replicated KV fallback, but
  service                             implementation;   weaker project
                                      less demonstrable depth
                                      custom            
                                      integration       
  -----------------------------------------------------------------------

**Decision:** use `go.etcd.io/etcd/raft/v3` for the consensus protocol
and implement a FlowForge-specific replicated key-value state machine,
storage layer, client API, snapshots, and integration.

Be explicit in documentation: FlowForge uses the etcd Raft library; it
does not claim to have invented or independently implemented the Raft
consensus algorithm.

If implementing Raft from scratch is selected as a separate educational
exercise, keep it isolated from the main critical path and test it
heavily.

## 6.2 Raft KV vs. Kafka-style log

These solve different problems.

  -----------------------------------------------------------------------
  Concern                 Raft-backed KV          Kafka
  ----------------------- ----------------------- -----------------------
  Primary purpose         Consistent cluster      Durable event streaming
                          coordination            

  Typical data            Leader, lease, worker   Market events, status
                          metadata, config        events, stream records

  Access pattern          Read/write current      Append and consume
                          coordinated state       ordered partitions

  Ordering                Consensus log order     Per-partition record
                                                  order

  Replay                  State snapshots and log Consumer offsets and
                          recovery                retained event log

  Best use in FlowForge   Control-plane           Data-plane event
                          coordination            transport
  -----------------------------------------------------------------------

**Decision:** use Raft for control-plane metadata and Kafka for
data-plane event streams. Do not build a Kafka clone merely because the
project includes Raft.

## 6.3 Workflow engine implementation options

  -----------------------------------------------------------------------
  Approach                Advantages              Tradeoffs
  ----------------------- ----------------------- -----------------------
  Build a focused DAG     Directly demonstrates   Must define durable
  workflow engine         scheduler, task state,  semantics carefully
                          retries, worker         
                          coordination            

  Integrate Temporal      Strong durable          FlowForge would become
                          execution model and     more of an
                          production patterns     integration/demo than a
                                                  workflow runtime

  Use Airflow             Familiar scheduled data Less aligned with
                          pipeline orchestration  building a general
                                                  distributed task
                                                  runtime

  Use a simple in-memory  Very quick prototype    No durable recovery;
  queue                                           insufficient for the
                                                  project's core goal
  -----------------------------------------------------------------------

**Decision:** build a focused workflow runtime. Study Temporal's durable
execution and task queue concepts, but do not claim parity with
Temporal.

## 6.4 Stream processor: Go vs. Flink

  -----------------------------------------------------------------------
  Approach                Advantages              Tradeoffs
  ----------------------- ----------------------- -----------------------
  Go consumer/processor   Lower setup complexity; Must implement windows,
                          strong control over     state handling, and
                          concurrency, batching,  recovery semantics
                          and backpressure; easy  
                          to integrate with       
                          FlowForge               

  Apache Flink            Mature stateful stream  More infrastructure and
                          processing,             conceptual complexity;
                          checkpointing, and      may distract from the
                          event-time support      runtime

  Kafka Streams           Integrated Kafka        Java ecosystem and less
                          stream-processing model direct alignment with
                                                  the Go core

  Spark Structured        Broad data processing   Heavier for this
  Streaming               ecosystem               portfolio's initial
                                                  workload
  -----------------------------------------------------------------------

**Decision:** start with a Go stream processor. Add Flink later only if
it demonstrates a meaningful capability such as checkpointed stateful
processing or complex event-time windows.

## 6.5 AI orchestration: FlowForge vs. LangGraph

  -----------------------------------------------------------------------
  Layer                               Responsibility
  ----------------------------------- -----------------------------------
  FlowForge                           Durable outer workflow, scheduling,
                                      worker execution, retries, and
                                      audit history

  LangGraph                           Stateful sequence of reasoning and
                                      tool calls inside one AI
                                      investigation

  LangChain                           Integrations for models,
                                      embeddings, retrievers, document
                                      loaders, and tools
  -----------------------------------------------------------------------

**Decision:** FlowForge schedules an `AI_INVESTIGATION` task. A Python
worker calls the LangGraph service. LangGraph completes its bounded
investigation and returns a structured result. FlowForge persists that
result and continues the parent workflow.

Do not make LangGraph a replacement for FlowForge's workflow scheduler.

## 6.6 Market-data source options

  -----------------------------------------------------------------------
  Source            Pros              Cons              Recommended use
  ----------------- ----------------- ----------------- -----------------
  Synthetic         Deterministic,    Not real market   Required for
  generator         repeatable,       data              baseline tests
                    supports fault                      
                    and load tests                      

  Historical        Easy to replay    Not live          Good first
  CSV/JSON data     and inspect                         demonstration

  Public API        More realistic    Rate limits,      Optional adapter
                                      credentials,      
                                      availability,     
                                      changing terms    

  Paid real-time    Most realistic    Cost and access   Not required
  feed                                constraints       
  -----------------------------------------------------------------------

Use synthetic and historical data first. Keep data-source adapters
separate so a live provider can be added without changing the processor.

## 6.7 Database options

-   **PostgreSQL:** primary durable store for workflow definitions,
    runs, tasks, attempts, approvals, and audit events.
-   **TimescaleDB:** time-series extension/database for market
    aggregates and time-indexed operational measurements.
-   **pgvector:** vector search for runbooks and platform documentation;
    it can share the PostgreSQL deployment initially.
-   **Redis:** optional cache or ephemeral acceleration layer. Do not
    make Redis the source of truth for workflow state or consensus
    metadata.

Avoid adding a database unless a concrete access pattern justifies it.

------------------------------------------------------------------------

# 7. Detailed component scope

## 7.1 Workflow API

Responsibilities:

-   Create and retrieve workflow definitions.
-   Start workflow runs.
-   Pause, cancel, or request retry where supported.
-   Retrieve task and attempt details.
-   Expose health and readiness endpoints.
-   Enforce request validation and authorization.
-   Return stable error formats.

## 7.2 DAG validator

A workflow is a directed acyclic graph. Each task has an ID, task type,
configuration, and dependency list.

Validation should reject:

-   Duplicate task IDs
-   Unknown dependencies
-   Self-dependencies
-   Cycles
-   Invalid retry policies
-   Missing task types
-   Invalid timeout values
-   Invalid workflow configuration

Use topological sorting or depth-first cycle detection.

## 7.3 Scheduler

Responsibilities:

-   Find tasks whose dependencies have completed successfully.
-   Apply concurrency limits.
-   Select eligible workers or queues.
-   Create task attempts.
-   Dispatch work.
-   Track deadlines and retry eligibility.
-   Avoid dispatching the same active attempt repeatedly.
-   Respect scheduler leadership.
-   Recover scheduled work after restart.

The scheduler must distinguish between a task that is ready, queued,
running, retry-waiting, successful, failed, cancelled, or blocked.

## 7.4 Worker runtime

Each worker should:

-   Register with the control plane.
-   Advertise supported task types and capacity.
-   Acquire or renew a lease.
-   Poll or receive task assignments.
-   Execute a task with a deadline/cancellation context.
-   Send heartbeats.
-   Report success or failure.
-   Include structured error details.
-   Shut down gracefully.
-   Re-register after restart.

Workers should be stateless where practical. Durable task state belongs
in the control plane.

## 7.5 Recovery manager

Recovery responsibilities include:

-   Detect expired worker leases.
-   Identify tasks that were running on a lost worker.
-   Determine whether an attempt can safely be retried.
-   Mark the previous attempt as timed out or abandoned.
-   Create a new attempt according to policy.
-   Prevent stale workers from committing results after losing
    ownership.
-   Reconcile database state with coordination metadata.

A lease expiry does not automatically mean the old process stopped. Use
fencing tokens or monotonically increasing attempt/ownership versions so
a stale worker cannot overwrite a newer result.

## 7.6 Retry policy

Support at least:

-   Maximum attempts
-   Fixed delay
-   Exponential backoff
-   Maximum backoff
-   Retryable vs. non-retryable error classification
-   Per-task timeout
-   Workflow-level timeout

A sensible initial default is exponential backoff with jitter. Make
defaults explicit and configurable.

## 7.7 Worker queues and task routing

Workers can advertise capabilities such as:

-   `market-data-ingest`
-   `market-data-process`
-   `database-maintenance`
-   `ai-investigation`

A task should only be assigned to a worker that supports its task type
and has capacity.

## 7.8 Dashboard

The dashboard should show:

-   Workflow definitions
-   Workflow run list and status
-   DAG graph with task states
-   Task attempts and error details
-   Worker health and capacity
-   Scheduler leader and cluster health
-   Kafka consumer lag
-   Market-data freshness and processing rates
-   AI investigation reports
-   Approval requests and decisions
-   Logs/traces links where available

The UI is a control and observability surface, not the main engineering
differentiator. Prioritize correctness and backend behavior before
polishing it.

------------------------------------------------------------------------

# 8. Raft-backed replicated key-value store

## 8.1 Purpose

Build a small replicated key-value coordination service in Go.

It should expose operations such as:

-   `Get(key)`
-   `Put(key, value)`
-   `Delete(key)`
-   `List(prefix)` (optional)
-   `Health()`
-   `ClusterStatus()`

The cluster should run three nodes in Docker Compose.

## 8.2 What state belongs in the KV store?

Good candidates:

-   Current scheduler leader/epoch
-   Worker registration metadata
-   Worker lease records
-   Scheduler ownership of a queue or partition
-   Cluster configuration
-   Coordination locks or ownership records

Do not store every market tick, full task logs, or the entire workflow
history in Raft.

## 8.3 Three-node quorum

With three voting nodes, a majority is two.

-   The cluster can continue making progress with one node unavailable,
    assuming the remaining two can communicate.
-   If two nodes are unavailable, the remaining node cannot form a
    majority and should not accept consensus writes.
-   A partition that leaves one node on one side and two nodes on the
    other should allow the majority side to progress; the minority side
    must not accept conflicting writes.

## 8.4 Raft integration architecture

``` mermaid
flowchart TB
    Client["FlowForge Coordination Client"] --> Node1["Raft Node 1"]
    Node1 <--> Node2["Raft Node 2"]
    Node2 <--> Node3["Raft Node 3"]
    Node1 <--> Node3
    Node1 --> WAL1["Persistent Raft State"]
    Node2 --> WAL2["Persistent Raft State"]
    Node3 --> WAL3["Persistent Raft State"]
    Node1 --> FSM1["KV State Machine"]
    Node2 --> FSM2["KV State Machine"]
    Node3 --> FSM3["KV State Machine"]
    FSM1 --> Snap1["Snapshot"]
    FSM2 --> Snap2["Snapshot"]
    FSM3 --> Snap3["Snapshot"]
```

Each node needs:

-   Node identity
-   Raft protocol instance
-   Transport to other nodes
-   Persistent storage for required Raft state and entries
-   Apply loop for committed entries
-   State machine
-   Snapshot creation and restore
-   Membership/configuration handling appropriate to the chosen scope
-   Metrics and health information

## 8.5 Suggested implementation structure

-   Use `go.etcd.io/etcd/raft/v3` for the consensus state machine.
-   Implement a FlowForge-specific KV state machine.
-   Persist Raft entries and hard state before acknowledging the
    corresponding protocol progress, following the library's documented
    lifecycle.
-   Apply committed commands to the KV state machine in order.
-   Add snapshots so the log does not grow indefinitely.
-   Expose client operations over gRPC.
-   Use Protocol Buffers for command and response messages.
-   Use BoltDB or Badger for local persistent state, or use a simple
    durable file-backed implementation for an early prototype.

The exact storage choice should be kept behind an interface so it can
change without rewriting the consensus logic.

## 8.6 Suggested key patterns

  -----------------------------------------------------------------------------
  Key pattern                   Example value           Purpose
  ----------------------------- ----------------------- -----------------------
  `/cluster/scheduler/leader`   node ID + term/epoch    Current scheduler
                                                        leader

  `/workers/{worker_id}`        capabilities + status + Worker registration
                                lease version           

  `/leases/{resource}`          owner + expiry +        Resource ownership
                                fencing token           

  `/queues/{queue}/owner`       scheduler ID + epoch    Queue ownership

  `/cluster/config`             versioned configuration Shared coordination
                                                        config
  -----------------------------------------------------------------------------

Lease expiration requires a clear clock and renewal model. Do not rely
only on a worker's local clock. The control plane should evaluate lease
expiry consistently and use fencing/version checks when assigning work.

## 8.7 Failure scenarios to test

1.  Kill the leader.
2.  Kill one follower.
3.  Restart a follower.
4.  Kill two nodes.
5.  Restore a node with stale local data.
6.  Introduce network delay.
7.  Introduce a network partition.
8.  Restart all nodes.
9.  Attempt writes without quorum.
10. Verify state-machine consistency after recovery.
11. Verify snapshots can restore a node.
12. Verify a stale scheduler cannot act as leader after a new term/epoch
    is established.

## 8.8 Important correctness rules

-   Never accept a write as committed before the consensus layer says it
    is committed.
-   Apply committed entries in log order.
-   Persist the required Raft state according to the library's contract.
-   Do not allow a minority partition to accept conflicting writes.
-   Use leader term/epoch or fencing tokens to reject stale owners.
-   Test restart and snapshot recovery, not just leader election.
-   Document what consistency guarantees the client API provides.

------------------------------------------------------------------------

# 9. Workflow orchestration engine

## 9.1 Workflow definition example

A workflow can be represented as JSON:

``` json
{
  "name": "market-data-hourly",
  "version": 1,
  "tasks": [
    {
      "id": "fetch",
      "type": "market-data-ingest",
      "depends_on": [],
      "queue": "market-data",
      "timeout_seconds": 120,
      "retry": {
        "max_attempts": 3,
        "strategy": "exponential"
      }
    },
    {
      "id": "validate",
      "type": "market-data-validate",
      "depends_on": ["fetch"],
      "timeout_seconds": 60
    },
    {
      "id": "process",
      "type": "market-data-process",
      "depends_on": ["validate"],
      "timeout_seconds": 300
    },
    {
      "id": "quality-check",
      "type": "market-data-quality-check",
      "depends_on": ["process"],
      "timeout_seconds": 60
    }
  ]
}
```

This is an illustrative contract; refine field names and validation
rules during implementation.

## 9.2 DAG execution model

``` mermaid
flowchart TD
    A["Fetch Market Data"] --> B["Validate Events"]
    B --> C["Publish to Kafka"]
    C --> D["Process Stream"]
    D --> E["Write Aggregates"]
    E --> F["Quality Check"]
    F --> G{"Healthy?"}
    G -->|Yes| H["Complete Workflow"]
    G -->|No| I["AI Investigation"]
    I --> J{"Human Approval Needed?"}
    J -->|Approve| K["Retry / Replay"]
    J -->|Reject| L["Escalate / Close"]
    K --> F
```

## 9.3 Task state model

Suggested task states:

-   `PENDING`
-   `READY`
-   `QUEUED`
-   `RUNNING`
-   `RETRY_WAIT`
-   `SUCCEEDED`
-   `FAILED`
-   `TIMED_OUT`
-   `CANCELLED`
-   `SKIPPED`

Suggested workflow states:

-   `CREATED`
-   `RUNNING`
-   `PAUSED`
-   `SUCCEEDED`
-   `FAILED`
-   `CANCELLED`
-   `NEEDS_ATTENTION`

Define allowed state transitions explicitly. Reject invalid transitions.

## 9.4 At-least-once execution and idempotency

A distributed worker system commonly needs to assume that a task may
execute more than once. A worker may finish its work but fail before
reporting success; the scheduler may then retry the task.

Therefore:

-   Design task handlers to be idempotent where possible.
-   Give each task attempt a unique ID.
-   Include an idempotency key for external side effects.
-   Store task result metadata transactionally.
-   Use fencing tokens or attempt versions to prevent stale attempts
    from committing.
-   Document the system's delivery and execution semantics.

Do not casually claim exactly-once task execution. A realistic first
version should describe at-least-once execution with idempotent handlers
and deduplicated side effects where supported.

## 9.5 Durable execution

Workflow state must survive:

-   API restart
-   Scheduler restart
-   Worker restart
-   Temporary database outage
-   Kafka outage
-   Network interruption

PostgreSQL is the durable record for workflow definitions, workflow
runs, tasks, attempts, and event history. The Raft KV store is for
coordinated cluster metadata.

## 9.6 Scheduler leadership

Only the active scheduler leader should make authoritative scheduling
decisions for a given scheduling domain.

A scheduler should:

1.  Discover or acquire leadership using the coordination service.
2.  Maintain a leader term/epoch.
3.  Renew ownership/lease.
4.  Stop making new authoritative assignments when leadership is lost.
5.  Ensure dispatched work includes a fencing/ownership version.
6.  Allow a new leader to reconstruct scheduling state from PostgreSQL
    and coordination metadata.

The initial version can use one scheduler process. Add multiple
scheduler processes after the single-node workflow engine works.

------------------------------------------------------------------------

# 10. Kafka streaming and market-data pipeline

## 10.1 Pipeline architecture

``` mermaid
flowchart LR
    Source["Synthetic / Historical Market Feed"] --> Producer["Go Kafka Producer"]
    Producer --> Topic[("Kafka: market.ticks")]
    Topic --> Consumer["Go Stream Processor"]
    Consumer --> Validate["Validate + Normalize"]
    Validate --> Dedupe["Deduplicate"]
    Dedupe --> Window["Window Aggregations"]
    Window --> Aggregates["OHLCV / VWAP / Volatility"]
    Aggregates --> TSDB[("TimescaleDB")]
    TSDB --> API["Analytics API"]
    API --> Dashboard["React Dashboard"]
    FF["FlowForge Scheduler"] --> Producer
    FF --> Consumer
    FF --> Monitor["Lag / Freshness Monitor"]
```

## 10.2 Event schema

Example market tick:

``` json
{
  "event_id": "evt_01J...",
  "symbol": "MSFT",
  "event_time": "2026-11-02T14:30:00.123Z",
  "ingested_at": "2026-11-02T14:30:00.250Z",
  "price": 512.34,
  "size": 100,
  "source": "synthetic",
  "sequence": 123456
}
```

Important fields:

-   `event_id`: stable unique event identifier for deduplication.
-   `symbol`: partition key.
-   `event_time`: time the market event occurred.
-   `ingested_at`: time FlowForge received the event.
-   `price`: event price.
-   `size`: quantity.
-   `source`: provider or generator.
-   `sequence`: optional source sequence for ordering checks.

## 10.3 Kafka topic design

Initial topics:

  -------------------------------------------------------------------------------
  Topic                           Key                     Purpose
  ------------------------------- ----------------------- -----------------------
  `market.ticks`                  symbol                  Raw normalized market
                                                          events

  `market.invalid`                event ID or symbol      Invalid events /
                                                          dead-letter path

  `market.aggregates`             symbol                  Computed aggregate
                                                          events

  `flowforge.workflow-events`     workflow ID             Workflow lifecycle
                                                          events

  `flowforge.task-events`         task ID                 Task lifecycle events

  `flowforge.ai-investigations`   workflow ID             Investigation
                                                          requests/results
                                                          (optional)
  -------------------------------------------------------------------------------

Partition `market.ticks` by symbol so events for a given symbol are
ordered within a partition. Do not assume total ordering across all
symbols or partitions.

## 10.4 Processing operations

The initial Go processor should implement:

1.  Schema validation
2.  Normalization
3.  Deduplication
4.  Out-of-order event handling
5.  Time-window aggregation
6.  Persistence
7.  Dead-letter handling
8.  Consumer offset management
9.  Metrics and health reporting

Suggested initial aggregates:

-   OHLCV: open, high, low, close, volume
-   VWAP: volume-weighted average price
-   Moving average
-   Rolling volatility
-   Event count and data freshness
-   Simple anomaly indicators

Start with one or two fixed window sizes, such as one minute and five
minutes. Add event-time windows and late-event policies after the basic
processor is stable.

## 10.5 Backpressure

Backpressure is a key demonstration of real streaming engineering.

Implement and measure:

-   Bounded Go channels between processing stages.
-   Bounded batch sizes.
-   Maximum in-flight records.
-   Kafka consumer pause/resume when downstream capacity is exhausted.
-   Controlled database write concurrency.
-   Consumer lag monitoring.
-   Processing latency and queue-depth metrics.
-   Graceful shutdown and offset commits.

Do not allow an unbounded in-memory queue to hide a slow downstream
database.

## 10.6 Replay and backfill

The system should support replay from:

-   A stored Kafka offset.
-   A timestamp, translated to the appropriate partition offsets.
-   A selected symbol or set of symbols.
-   A selected historical time range.

Use a separate consumer group for a backfill/replay job where practical.
Keep replay processing idempotent, for example by using stable aggregate
keys and database upserts.

A replay should not silently overwrite newer results with stale data.
Define how aggregate versions, source timestamps, and upsert conflict
rules work.

## 10.7 TimescaleDB schema direction

A possible first table:

`market_ticks`:

-   `event_time`
-   `symbol`
-   `event_id`
-   `price`
-   `size`
-   `source`
-   `sequence`
-   `ingested_at`

A possible aggregate table:

`market_ohlcv`:

-   `window_start`
-   `window_size`
-   `symbol`
-   `open`
-   `high`
-   `low`
-   `close`
-   `volume`
-   `vwap`
-   `event_count`
-   `last_updated_at`

Use a composite uniqueness constraint such as
`(symbol, window_size, window_start)` for aggregate upserts. Exact
TimescaleDB hypertable and indexing choices should be based on the final
schema and query patterns.

## 10.8 Streaming failure scenarios

Test:

-   Kafka broker becomes unavailable.
-   Consumer process stops.
-   Database writes become slow.
-   Duplicate events arrive.
-   Events arrive out of order.
-   A processor restarts after processing but before committing offsets.
-   One partition becomes much hotter than others.
-   A replay overlaps with live processing.
-   Invalid events arrive.
-   A consumer group rebalances.

Measure recovery time, duplicate behavior, lag growth, and final
aggregate correctness.

------------------------------------------------------------------------

# 11. AI Operations Copilot: RAG, LangChain, and LangGraph

## 11.1 Why AI Ops instead of another finance chatbot?

The project already has a separate investment-research application
concept. FlowForge should not repeat the same product idea with another
financial RAG chatbot.

The AI feature should strengthen the distributed-systems story:

> **An AI Operations Copilot that uses workflow state, logs, metrics,
> and runbooks to investigate incidents and recommend safe recovery
> actions.**

This gives the AI feature a meaningful reason to exist within FlowForge.

## 11.2 What the copilot should answer

Examples:

-   Why did the market-data pipeline fail yesterday at 2:30 PM?
-   Why is workflow `run-123` taking 40% longer than normal?
-   Which task is responsible for the current delay?
-   Is the consumer lag growing or recovering?
-   Did the worker lose its lease before the task failed?
-   Which runbook applies to this error?
-   What is the safest next recovery step?
-   What would happen if this task were replayed?

The system should distinguish observed facts from hypotheses and
recommendations.

## 11.3 RAG knowledge base

Ingest documents such as:

-   FlowForge architecture documentation
-   API and event schemas
-   Worker configuration references
-   Error-code catalog
-   Kafka troubleshooting guide
-   Raft/cluster runbooks
-   Database troubleshooting notes
-   Deployment and rollback runbooks
-   Incident postmortems generated for test scenarios
-   Operational procedures

Store document metadata with each chunk:

-   Document ID
-   Title
-   Source path
-   Version
-   Section
-   Chunk index
-   Last updated time
-   Access scope
-   Optional tags

Use PostgreSQL + pgvector initially. Avoid introducing a separate vector
database until scale or feature needs justify it.

## 11.4 LangChain's role

Use LangChain for integrations such as:

-   Chat model clients
-   Embedding model clients
-   Document loaders
-   Text splitters
-   Vector-store adapters
-   Retriever interfaces
-   Tool interfaces

LangChain is an integration layer. It should not own FlowForge's durable
workflow state.

## 11.5 LangGraph's role

Use LangGraph for the internal investigation graph.

Suggested nodes:

1.  `parse_request`
2.  `identify_workflow`
3.  `load_execution_context`
4.  `retrieve_runbooks`
5.  `inspect_logs`
6.  `inspect_metrics`
7.  `analyze_evidence`
8.  `validate_hypothesis`
9.  `generate_recommendation`
10. `human_approval` (when required)
11. `finalize_report`

Example graph:

``` mermaid
flowchart TD
    START([Start]) --> Parse["Parse incident question"]
    Parse --> Context["Load workflow context"]
    Context --> Retrieve["Retrieve runbooks / docs"]
    Retrieve --> Logs["Inspect logs"]
    Logs --> Metrics["Inspect metrics"]
    Metrics --> Analyze["Correlate evidence"]
    Analyze --> Valid{"Evidence sufficient?"}
    Valid -->|No| More["Gather additional evidence"]
    More --> Analyze
    Valid -->|Yes| Recommend["Generate recommendation"]
    Recommend --> Approval{"Action requires approval?"}
    Approval -->|Yes| Human["Wait for human decision"]
    Approval -->|No| Report["Create investigation report"]
    Human --> Report
    Report --> END([End])
```

## 11.6 Tools available to the agent

The agent should use typed, bounded tools rather than arbitrary database
access.

Potential tools:

-   `get_workflow_run(workflow_id, run_id)`
-   `list_task_attempts(run_id, task_id)`
-   `get_task_logs(run_id, task_id, attempt_id)`
-   `get_worker_status(worker_id)`
-   `get_scheduler_leader()`
-   `get_kafka_consumer_lag(group_id, topic)`
-   `get_pipeline_metrics(pipeline_id, time_range)`
-   `get_database_latency(time_range)`
-   `search_runbooks(query, filters)`
-   `create_replay_proposal(...)`
-   `request_retry_approval(...)`

Read-only tools should be implemented first. Any write or recovery
action must be guarded by authorization and human approval.

## 11.7 Structured investigation result

Return structured output rather than only free-form prose.

Example:

``` json
{
  "investigation_id": "inv_123",
  "workflow_id": "wf_market_data",
  "run_id": "run_123",
  "summary": "The processor stopped making progress while database write latency increased.",
  "observed_facts": [
    {
      "fact": "Consumer lag increased during the failure window.",
      "source": "kafka_consumer_lag",
      "timestamp": "2026-11-02T14:30:00Z"
    },
    {
      "fact": "Database write latency exceeded the configured threshold.",
      "source": "database_metrics",
      "timestamp": "2026-11-02T14:30:00Z"
    }
  ],
  "hypotheses": [
    {
      "description": "The processor was backpressured by slow database writes.",
      "confidence": "medium",
      "supporting_evidence": [
        "Consumer lag increased",
        "Database write latency increased"
      ]
    }
  ],
  "recommendations": [
    {
      "action": "Inspect database capacity and retry the affected processing window.",
      "risk": "medium",
      "requires_approval": true
    }
  ],
  "limitations": [
    "The available telemetry does not establish the exact database root cause."
  ]
}
```

The model should never fabricate metrics, logs, or evidence. Every
factual statement should refer to retrieved evidence or be labeled as a
hypothesis.

## 11.8 Human-in-the-loop

Use approval gates for operations such as:

-   Replaying a production-like time range
-   Cancelling a workflow
-   Increasing worker capacity
-   Changing retry configuration
-   Reprocessing stored data
-   Triggering a large backfill

The first release can make the copilot recommendation-only. Then add a
simulated approval workflow and safe actions.

## 11.9 AI reliability and cost controls

-   Persist investigation request IDs.
-   Persist inputs and structured outputs.
-   Make AI tasks retry-aware.
-   Avoid repeated model calls when a completed result can be reused.
-   Bound tool call counts and timeouts.
-   Use retrieval filters to constrain context.
-   Include citations or document references in the result.
-   Return an explicit insufficient-evidence outcome.
-   Keep prompts, model configuration, and evaluation cases versioned.
-   Use deterministic mock LLM responses in tests.
-   Separate test/evaluation mode from live model mode.

------------------------------------------------------------------------

# 12. Data model and event contracts

The following is a proposed starting model. It is not a final migration
script; refine constraints and indexes as implementation begins.

## 12.1 Core PostgreSQL entities

### `workflow_definitions`

  Field               Type direction   Notes
  ------------------- ---------------- ----------------------
  `id`                UUID             Primary key
  `name`              text             Human-readable name
  `version`           integer          Versioned definition
  `definition_json`   JSONB            DAG/task definition
  `created_at`        timestamp        Creation time
  `updated_at`        timestamp        Last update

Unique constraint: `(name, version)`.

### `workflow_runs`

  Field                      Type direction       Notes
  -------------------------- -------------------- ------------------------------
  `id`                       UUID                 Run ID
  `workflow_definition_id`   UUID                 Definition reference
  `status`                   text/enum            Workflow state
  `input_json`               JSONB                Run input
  `started_at`               timestamp            Start time
  `completed_at`             timestamp nullable   Completion
  `created_at`               timestamp            Creation
  `leader_epoch`             bigint nullable      Scheduling ownership version

### `workflow_tasks`

  Field               Type direction      Notes
  ------------------- ------------------- --------------------------
  `id`                UUID                Task instance ID
  `workflow_run_id`   UUID                Run reference
  `task_key`          text                Definition-level task ID
  `task_type`         text                Worker capability
  `status`            text/enum           Current state
  `queue_name`        text                Routing queue
  `depends_on`        JSONB or relation   Dependencies
  `input_json`        JSONB               Task input
  `result_json`       JSONB nullable      Task output
  `available_at`      timestamp           Retry/scheduling time
  `created_at`        timestamp           Creation
  `updated_at`        timestamp           Last state update

### `task_attempts`

  Field              Type direction       Notes
  ------------------ -------------------- -----------------------------
  `id`               UUID                 Attempt ID
  `task_id`          UUID                 Task reference
  `attempt_number`   integer              Sequential attempt
  `worker_id`        text                 Assigned worker
  `status`           text/enum            Attempt result
  `started_at`       timestamp            Start
  `finished_at`      timestamp nullable   Finish
  `error_code`       text nullable        Stable error classification
  `error_message`    text nullable        Sanitized details
  `fencing_token`    bigint nullable      Ownership version
  `result_json`      JSONB nullable       Result metadata

### `workflow_events`

Append-oriented event history:

-   `id`
-   `workflow_run_id`
-   `task_id` nullable
-   `event_type`
-   `event_version`
-   `payload_json`
-   `created_at`
-   `correlation_id`

### `worker_observations`

Optional operational snapshot table:

-   `worker_id`
-   `observed_at`
-   `status`
-   `capacity`
-   `active_tasks`
-   `supported_task_types`
-   `metadata_json`

### `approval_requests`

-   `id`
-   `workflow_run_id`
-   `investigation_id`
-   `action_type`
-   `proposal_json`
-   `status`
-   `requested_at`
-   `decided_at`
-   `decided_by`
-   `decision_reason`

## 12.2 Event envelope

Use a consistent event envelope for Kafka and internal event
publication:

``` json
{
  "event_id": "evt_01J...",
  "event_type": "task.attempt.failed",
  "event_version": 1,
  "occurred_at": "2026-11-02T14:30:00Z",
  "producer": "flowforge-worker",
  "correlation_id": "run_123",
  "aggregate_id": "task_456",
  "payload": {
    "workflow_run_id": "run_123",
    "task_id": "task_456",
    "attempt_id": "attempt_2",
    "error_code": "DATABASE_TIMEOUT"
  }
}
```

Requirements:

-   Version event schemas.
-   Include stable IDs.
-   Include correlation identifiers.
-   Define compatibility rules.
-   Avoid publishing secrets or sensitive data.
-   Make consumers tolerate unknown optional fields.

------------------------------------------------------------------------

# 13. API design

The API list below is an initial contract direction. Implement a small
subset first and version it as the project evolves.

## 13.1 Workflow REST endpoints

  ------------------------------------------------------------------------------------
  Method                  Endpoint                             Purpose
  ----------------------- ------------------------------------ -----------------------
  `POST`                  `/api/v1/workflows`                  Create workflow
                                                               definition

  `GET`                   `/api/v1/workflows`                  List definitions

  `GET`                   `/api/v1/workflows/{id}`             Get definition

  `POST`                  `/api/v1/workflows/{id}/runs`        Start a run

  `GET`                   `/api/v1/runs`                       List runs

  `GET`                   `/api/v1/runs/{run_id}`              Get run

  `POST`                  `/api/v1/runs/{run_id}/cancel`       Request cancellation

  `POST`                  `/api/v1/runs/{run_id}/retry`        Request retry where
                                                               supported

  `GET`                   `/api/v1/runs/{run_id}/tasks`        List tasks

  `GET`                   `/api/v1/tasks/{task_id}/attempts`   List attempts

  `GET`                   `/api/v1/workers`                    List worker status

  `GET`                   `/api/v1/cluster/status`             Cluster health

  `GET`                   `/healthz`                           Liveness

  `GET`                   `/readyz`                            Readiness
  ------------------------------------------------------------------------------------

## 13.2 Coordination gRPC direction

Potential RPCs:

-   `Get`
-   `Put`
-   `Delete`
-   `List`
-   `GetClusterStatus`
-   `RegisterWorker`
-   `RenewLease`
-   `ReleaseLease`

Keep the low-level KV API separate from higher-level worker/lease
semantics where possible. The FlowForge coordination client can provide
typed methods over the underlying KV operations.

## 13.3 Worker protocol

Potential worker operations:

-   Register
-   Heartbeat
-   Poll/Receive task
-   Acknowledge task
-   Report progress
-   Complete task
-   Fail task
-   Renew lease

A worker response should include:

-   Task ID
-   Attempt ID
-   Attempt number
-   Input payload
-   Deadline
-   Fencing token/ownership epoch
-   Trace/correlation ID

## 13.4 AI service endpoints

  Method   Endpoint                           Purpose
  -------- ---------------------------------- -----------------------------
  `POST`   `/ai/v1/investigations`            Start investigation
  `GET`    `/ai/v1/investigations/{id}`       Get result/status
  `POST`   `/ai/v1/knowledge/ingest`          Ingest or refresh documents
  `GET`    `/ai/v1/knowledge/search`          Search runbooks
  `POST`   `/ai/v1/approvals/{id}/decision`   Record approval decision

The AI service should be callable internally by a FlowForge worker. It
should not bypass the FlowForge task lifecycle.

------------------------------------------------------------------------

# 14. Repository structure

A monorepo is recommended because the components are tightly integrated
and the project is easier to run and review as one system.

``` text
flowforge/
├── README.md
├── LICENSE
├── Makefile
├── docker-compose.yml
├── .env.example
├── .gitignore
├── docs/
│   ├── architecture/
│   │   ├── overview.md
│   │   ├── control-plane.md
│   │   ├── data-plane.md
│   │   ├── ai-plane.md
│   │   └── diagrams/
│   ├── decisions/
│   │   ├── ADR-001-go-core.md
│   │   ├── ADR-002-raft-library.md
│   │   ├── ADR-003-kafka.md
│   │   ├── ADR-004-postgres-timescale.md
│   │   └── ADR-005-langgraph-boundary.md
│   ├── runbooks/
│   │   ├── worker-unavailable.md
│   │   ├── scheduler-failover.md
│   │   ├── kafka-consumer-lag.md
│   │   ├── database-latency.md
│   │   └── replay-market-data.md
│   ├── api/
│   └── testing/
├── cmd/
│   ├── flowforge-api/
│   │   └── main.go
│   ├── flowforge-scheduler/
│   │   └── main.go
│   ├── flowforge-worker/
│   │   └── main.go
│   ├── flowforge-raft/
│   │   └── main.go
│   ├── marketdata-producer/
│   │   └── main.go
│   └── marketdata-processor/
│       └── main.go
├── internal/
│   ├── api/
│   ├── workflow/
│   ├── dag/
│   ├── scheduler/
│   ├── dispatcher/
│   ├── worker/
│   ├── workerregistry/
│   ├── recovery/
│   ├── coordination/
│   ├── raftkv/
│   │   ├── node/
│   │   ├── storage/
│   │   ├── transport/
│   │   ├── state_machine/
│   │   └── snapshot/
│   ├── persistence/
│   ├── events/
│   ├── kafka/
│   ├── marketdata/
│   ├── stream/
│   ├── timeseries/
│   └── observability/
├── pkg/
│   ├── client/
│   ├── protocol/
│   ├── retry/
│   └── errors/
├── proto/
│   ├── coordination/v1/
│   ├── worker/v1/
│   └── workflow/v1/
├── migrations/
│   ├── postgres/
│   └── timescale/
├── services/
│   └── ai-ops/
│       ├── pyproject.toml
│       ├── app/
│       │   ├── main.py
│       │   ├── api/
│       │   ├── agent/
│       │   ├── graph/
│       │   ├── retrieval/
│       │   ├── ingestion/
│       │   ├── tools/
│       │   ├── models/
│       │   └── config/
│       └── tests/
├── frontend/
│   ├── package.json
│   ├── src/
│   │   ├── pages/
│   │   ├── components/
│   │   ├── features/
│   │   ├── api/
│   │   └── types/
│   └── tests/
├── test/
│   ├── integration/
│   ├── fault/
│   ├── load/
│   └── fixtures/
├── scripts/
│   ├── dev-up.sh
│   ├── dev-down.sh
│   ├── seed-market-data.sh
│   └── chaos/
└── .github/
    └── workflows/
        ├── go.yml
        ├── python.yml
        └── frontend.yml
```

Do not create every directory at the beginning. Introduce folders when a
real component exists. This tree is the target organization, not a
requirement to scaffold empty folders on day one.

------------------------------------------------------------------------

# 15. Local development environment

## 15.1 Recommended local services

Start with:

-   Go API
-   Go scheduler
-   One Go worker
-   PostgreSQL
-   Docker Compose

Add incrementally:

-   Three Raft nodes
-   Kafka
-   TimescaleDB (or TimescaleDB image based on PostgreSQL)
-   Python AI service
-   pgvector
-   React frontend
-   Prometheus
-   Grafana
-   OpenTelemetry Collector

Redis is optional. Kubernetes is not required for the first working
release.

## 15.2 Suggested local startup experience

Aim for a small number of commands:

``` bash
# Start infrastructure
docker compose up -d postgres

# Run Go services during early development
go run ./cmd/flowforge-api
go run ./cmd/flowforge-scheduler
go run ./cmd/flowforge-worker

# Run tests
go test ./...

# Later, start the complete stack
docker compose up --build
```

The exact commands will change as the Makefile and Compose setup are
implemented.

## 15.3 Developer experience requirements

-   `.env.example` with documented variables.
-   No secrets committed to Git.
-   Database migrations run predictably.
-   Seed data is deterministic.
-   A single command runs unit tests.
-   A documented command runs integration tests.
-   Health checks exist for infrastructure and services.
-   Logs are structured and include correlation IDs.

------------------------------------------------------------------------

# 16. Testing and fault injection

Testing is a major differentiator. The project should prove behavior
under failure rather than relying only on a successful demo.

## 16.1 Test layers

  -----------------------------------------------------------------------
  Layer                               What to test
  ----------------------------------- -----------------------------------
  Unit                                DAG validation, state transitions,
                                      retry policy, aggregation
                                      calculations

  Component                           Scheduler with repository
                                      abstraction; worker handler
                                      behavior

  Integration                         API + PostgreSQL; Kafka
                                      producer/consumer; Raft nodes

  End-to-end                          Submit workflow and verify
                                      completion across workers

  Fault injection                     Kill nodes, partition network, stop
                                      workers, slow database

  Load                                Concurrent workflows, task
                                      throughput, event throughput

  AI evaluation                       Retrieval relevance, evidence
                                      grounding, structured output
                                      validity
  -----------------------------------------------------------------------

## 16.2 Workflow test cases

-   DAG with independent tasks executes in parallel where capacity
    permits.
-   Dependent task waits until dependencies succeed.
-   Invalid cycle is rejected.
-   Failed task retries within policy.
-   Non-retryable failure does not retry.
-   Timeout creates the correct task/attempt state.
-   Worker restart does not corrupt workflow state.
-   Scheduler restart reconstructs pending work.
-   Stale attempt cannot commit after a newer attempt owns the task.
-   Cancellation prevents new work and handles running tasks according
    to policy.

## 16.3 Raft test cases

-   Leader election occurs.
-   Leader failure results in a new leader when quorum exists.
-   A single surviving node cannot commit writes without quorum.
-   A follower catches up after restart.
-   Snapshots restore state.
-   Network partition does not allow conflicting committed writes.
-   FlowForge rejects stale scheduler epochs.

## 16.4 Kafka test cases

-   Consumer restart resumes from committed offset.
-   Duplicate event does not duplicate final aggregate.
-   Out-of-order event follows documented late-event policy.
-   Database outage increases lag but does not lose committed input.
-   Replay produces the expected aggregate state.
-   Poison event is routed to a dead-letter topic.
-   Rebalance does not corrupt aggregation state.

## 16.5 AI evaluation cases

Create a small incident evaluation set with known evidence and expected
outcomes.

Examples:

-   Worker lease expired before task completion.
-   Kafka lag increased due to slow database writes.
-   Raft quorum was unavailable.
-   A task repeatedly failed due to invalid input.
-   A runbook exists but telemetry is insufficient to determine root
    cause.

Measure:

-   Correct workflow/run identification
-   Correct tool selection
-   Retrieval relevance
-   Evidence coverage
-   Unsupported-claim rate
-   Recommendation safety
-   Structured schema validity
-   Human-approval enforcement

Use deterministic mocks for automated tests. Live LLM evaluation can be
a separate, manually run suite.

## 16.6 Fault injection methods

Start simple:

-   Stop a container.
-   Restart a worker.
-   Pause a service.
-   Add artificial processing delay.
-   Add database latency.
-   Kill a Raft leader.
-   Disconnect a node from the Docker network.
-   Increase event volume.

A more advanced chaos framework is optional. The goal is repeatable
tests with clear expected outcomes.

------------------------------------------------------------------------

# 17. Observability and operations

## 17.1 OpenTelemetry

Instrument:

-   HTTP requests
-   gRPC requests
-   Workflow runs
-   Task attempts
-   Worker execution
-   Kafka publish/consume
-   Database queries
-   AI investigation calls

Propagate trace context across task dispatch and event envelopes where
practical.

## 17.2 Prometheus metrics

Suggested metrics:

### Workflow metrics

-   `flowforge_workflows_started_total`
-   `flowforge_workflows_completed_total`
-   `flowforge_workflow_duration_seconds`
-   `flowforge_tasks_started_total`
-   `flowforge_tasks_retried_total`
-   `flowforge_task_duration_seconds`
-   `flowforge_scheduler_queue_depth`

### Worker metrics

-   `flowforge_workers_registered`
-   `flowforge_worker_active_tasks`
-   `flowforge_worker_lease_renewal_failures_total`
-   `flowforge_worker_task_failures_total`

### Raft metrics

-   `flowforge_raft_is_leader`
-   `flowforge_raft_term`
-   `flowforge_raft_commit_index`
-   `flowforge_raft_applied_index`
-   `flowforge_raft_proposals_pending`
-   `flowforge_raft_peer_unreachable`

Use the underlying Raft library's exposed metrics where appropriate,
while keeping FlowForge-specific metrics separate.

### Kafka and streaming metrics

-   `marketdata_events_consumed_total`
-   `marketdata_events_invalid_total`
-   `marketdata_events_deduplicated_total`
-   `marketdata_processing_latency_seconds`
-   `marketdata_consumer_lag`
-   `marketdata_batch_size`
-   `marketdata_channel_queue_depth`
-   `marketdata_database_write_latency_seconds`
-   `marketdata_last_event_timestamp`

### AI metrics

-   `flowforge_ai_investigations_total`
-   `flowforge_ai_investigation_duration_seconds`
-   `flowforge_ai_tool_calls_total`
-   `flowforge_ai_retrieval_results`
-   `flowforge_ai_approval_wait_seconds`

Do not expose sensitive prompts, credentials, or private payloads as
metric labels.

## 17.3 Dashboard panels

Suggested Grafana dashboards:

1.  Workflow throughput and duration
2.  Task success/failure/retry rates
3.  Scheduler queue depth
4.  Worker health and active capacity
5.  Raft leader, term, peer health, and quorum-related status
6.  Kafka consumer lag by topic/partition/group
7.  Market-data event rate and processing latency
8.  Database write latency
9.  AI investigation latency and tool usage

## 17.4 Runbooks

Create operational runbooks for:

-   Scheduler leader failure
-   Worker lease expiration
-   Stuck workflow
-   Kafka consumer lag
-   Kafka broker outage
-   Database write latency
-   Market-data replay
-   Raft quorum loss
-   AI investigation failure

These runbooks serve both operators and the AI RAG knowledge base.

------------------------------------------------------------------------

# 18. Security and reliability requirements

Even as a portfolio project, establish good boundaries.

## 18.1 Security

-   Keep credentials in environment variables or a secret manager.
-   Do not commit API keys or database passwords.
-   Validate all external input.
-   Use service-to-service authentication in a later hardening phase.
-   Restrict AI tools to explicit, typed operations.
-   Do not give the AI agent arbitrary shell or unrestricted SQL access.
-   Redact secrets from logs.
-   Separate read-only diagnosis from write/recovery operations.
-   Require explicit authorization and approval for disruptive actions.
-   Apply request size and timeout limits.

## 18.2 Reliability

-   Durable workflow state in PostgreSQL.
-   Consensus-backed coordination metadata.
-   Worker heartbeats and leases.
-   Fencing tokens for stale ownership.
-   Bounded retries and deadlines.
-   Idempotent task handlers where possible.
-   Kafka offset discipline.
-   Idempotent aggregate upserts.
-   Graceful shutdown.
-   Backpressure instead of unbounded buffering.
-   Explicit degraded behavior when dependencies are unavailable.

## 18.3 Consistency and delivery semantics

Document the consistency model for each subsystem:

-   Raft KV: committed writes require consensus quorum.
-   Workflow DB: transactionally persisted state transitions.
-   Worker execution: assume at-least-once attempts; protect side
    effects with idempotency and fencing.
-   Kafka: ordering is per partition; replay uses offsets.
-   Time-series aggregates: deterministic upserts and documented
    late-event behavior.
-   AI: recommendations are advisory unless approved and executed
    through FlowForge.

------------------------------------------------------------------------

# 19. Implementation phases and milestones

The order matters. Do not start by implementing every technology at
once.

A substantial portfolio version is estimated at roughly **4--5 months
part-time**, depending on weekly availability, depth of tests, and how
much UI polish is included. This is a planning estimate, not a
guaranteed timeline.

## Phase 0 --- Design and project foundation

**Goal:** create a clear, testable foundation.

Tasks:

-   Create repository.
-   Write README project overview.
-   Create initial architecture diagram.
-   Record initial architecture decisions (ADRs).
-   Set up Go module.
-   Set up Python project structure for later.
-   Set up linting and formatting.
-   Add GitHub Actions for basic Go tests.
-   Add Docker Compose with PostgreSQL.
-   Define initial workflow and task state machines.
-   Define the first workflow JSON contract.

Deliverables:

-   Repository and README
-   Initial ADRs
-   Basic CI
-   Local database setup
-   Workflow contract and state diagrams

Acceptance criteria:

-   A new developer can clone the repository and run the documented
    checks.
-   The architecture and first milestone are understandable without
    reading source code.

## Phase 1 --- Core workflow engine

`<badge>`{=html}Highest priority`</badge>`{=html}

**Goal:** execute a durable DAG across workers before adding distributed
consensus or streaming complexity.

Tasks:

1.  Implement workflow definition model.
2.  Implement DAG validation and cycle detection.
3.  Implement workflow and task persistence in PostgreSQL.
4.  Implement REST API to create definitions and start runs.
5.  Implement scheduler for ready tasks.
6.  Implement task dispatch to one worker.
7.  Implement worker registration and heartbeat.
8.  Implement task success/failure reporting.
9.  Implement retry policy and task timeouts.
10. Implement scheduler recovery after restart.
11. Add a basic CLI or minimal dashboard view.
12. Add end-to-end tests.

Deliverables:

-   Working Go API
-   Scheduler
-   Worker runtime
-   PostgreSQL migrations
-   Durable task history
-   Retry/timeout support
-   End-to-end workflow demo

Acceptance criteria:

-   A multi-step DAG completes successfully.
-   A failed task retries according to policy.
-   A scheduler restart does not lose pending workflow state.
-   A worker restart is handled without corrupting the run.
-   Invalid DAGs are rejected.

**Do not move on until this phase works reliably.**

## Phase 2 --- Raft-backed coordination

**Goal:** make control-plane coordination fault tolerant.

Tasks:

1.  Add three Raft nodes.
2.  Integrate `go.etcd.io/etcd/raft/v3`.
3.  Implement persistent Raft storage.
4.  Implement the replicated KV state machine.
5.  Implement gRPC client API.
6.  Implement leader/term tracking.
7.  Store scheduler leader/epoch.
8.  Add worker registration and lease records.
9.  Add fencing tokens.
10. Integrate scheduler leadership with FlowForge.
11. Add snapshots and restore.
12. Add failure and partition tests.

Deliverables:

-   Three-node local Raft cluster
-   Replicated KV API
-   Scheduler leadership/failover
-   Worker lease coordination
-   Fault-injection test suite
-   Raft architecture and failure documentation

Acceptance criteria:

-   A leader can fail and a new leader can take over with quorum.
-   Writes do not succeed without quorum.
-   A restarted follower catches up.
-   Stale scheduler ownership is rejected.
-   Snapshot/restart behavior is tested.

## Phase 3 --- Kafka and streaming market-data workload

**Goal:** demonstrate event-driven processing, replay, and backpressure.

Tasks:

1.  Add Kafka to Docker Compose.
2.  Implement synthetic market-data generator.
3.  Implement Kafka producer.
4.  Define and version event schema.
5.  Partition events by symbol.
6.  Implement Go consumer/processor.
7.  Add validation and normalization.
8.  Add deduplication.
9.  Implement one-minute OHLCV and VWAP.
10. Persist aggregates in TimescaleDB.
11. Add consumer lag and freshness metrics.
12. Add bounded queues and backpressure.
13. Add replay/backfill support.
14. Integrate producer/processor jobs with FlowForge.
15. Add failure tests for broker, consumer, and database outages.

Deliverables:

-   Kafka-backed event pipeline
-   Go stream processor
-   TimescaleDB aggregate tables
-   Replay/backfill capability
-   Backpressure and lag monitoring
-   End-to-end market-data workflow

Acceptance criteria:

-   Events flow from producer through Kafka to TimescaleDB.
-   Duplicate events do not create duplicate aggregates.
-   Replay produces expected deterministic results.
-   A slow database causes controlled backpressure rather than unbounded
    memory growth.
-   Processor restart and offset behavior are tested.

## Phase 4 --- AI Operations Copilot

**Goal:** make incident investigation evidence-based and integrated with
FlowForge.

Tasks:

1.  Create Python FastAPI service.
2.  Define investigation request/result schemas.
3.  Ingest architecture docs and runbooks.
4.  Chunk documents and create embeddings.
5.  Store vectors in PostgreSQL + pgvector.
6.  Implement LangChain retriever.
7.  Implement read-only FlowForge tools.
8.  Implement LangGraph investigation nodes.
9.  Query workflow/task history.
10. Query metrics and logs.
11. Correlate evidence.
12. Generate structured incident report.
13. Add evidence references and insufficient-evidence behavior.
14. Integrate as a FlowForge `AI_INVESTIGATION` task.
15. Add deterministic AI tests and evaluation cases.
16. Add approval flow for proposed recovery actions.

Deliverables:

-   AI service
-   RAG knowledge base
-   LangGraph investigation graph
-   Typed operational tools
-   Evidence-backed incident reports
-   FlowForge integration
-   Human approval mechanism

Acceptance criteria:

-   The copilot can identify a known test incident using real system
    state.
-   The report includes supporting evidence.
-   It does not invent logs or metrics.
-   It can state when evidence is insufficient.
-   It cannot execute disruptive actions without approval.

## Phase 5 --- Production-style engineering and polish

**Goal:** make the project credible, reproducible, and easy to evaluate.

Tasks:

1.  Add OpenTelemetry tracing.
2.  Add Prometheus metrics.
3.  Build Grafana dashboards.
4.  Add structured logging and correlation IDs.
5.  Add load tests.
6.  Add repeatable fault-injection scenarios.
7.  Add Docker Compose full-stack profile.
8.  Add optional Kubernetes manifests.
9.  Add security hardening.
10. Improve README and architecture docs.
11. Record demo videos or GIFs.
12. Capture benchmark methodology and results.
13. Publish known limitations and tradeoffs.

Deliverables:

-   Full local demo
-   Observability dashboards
-   Load and fault test reports
-   Architecture decision records
-   Runbooks
-   Demo artifacts
-   Clear project limitations

Acceptance criteria:

-   A reviewer can start the system from documented instructions.
-   The main demo is repeatable.
-   Benchmarks explain hardware, configuration, data size, and
    methodology.
-   Failure scenarios have expected and observed results.
-   No unsupported performance claims appear in the README.

## Phase dependency overview

``` mermaid
flowchart TD
    P0["Phase 0: Foundation"] --> P1["Phase 1: Workflow Engine"]
    P1 --> P2["Phase 2: Raft Coordination"]
    P1 --> P3["Phase 3: Kafka + Market Data"]
    P2 --> P3
    P3 --> P4["Phase 4: AI Ops Copilot"]
    P2 --> P4
    P4 --> P5["Phase 5: Production Engineering"]
    P3 --> P5
```

Some documentation and infrastructure work can overlap, but the core
workflow engine should precede the advanced features.

------------------------------------------------------------------------

# 20. Scope boundaries and stretch goals

## 20.1 Must-have for a strong first release

-   Go workflow API
-   DAG validation
-   Durable workflow/task persistence
-   Scheduler and worker execution
-   Retries and timeouts
-   Worker heartbeat and recovery
-   Three-node Raft-backed coordination
-   Leader failover and fencing
-   Kafka producer/consumer
-   Go market-data processor
-   TimescaleDB aggregates
-   Backpressure and replay
-   Basic AI Ops investigation with RAG and LangGraph
-   Evidence-based structured output
-   Unit, integration, and fault tests
-   OpenTelemetry/Prometheus basics
-   Clear README and architecture diagrams

## 20.2 Good stretch goals

-   Kubernetes deployment
-   Dynamic worker autoscaling
-   Multi-tenant workflow isolation
-   Per-tenant quotas and rate limits
-   Workflow version migration
-   Scheduled cron triggers
-   Priority queues
-   Workflow pause/resume
-   Webhook notifications
-   Advanced event-time processing
-   Apache Flink integration
-   Multi-region coordination
-   Raft membership changes
-   More advanced snapshot compaction
-   Pluggable storage backends
-   Policy-based AI action approvals
-   Cost and token usage tracking
-   Benchmark comparison across worker counts

## 20.3 Optional separate learning branches

These can be valuable but should not delay the core product:

-   A Raft implementation from scratch for educational comparison.
-   A small append-only log implementation to understand Kafka-like
    storage.
-   A custom scheduler benchmark.
-   A Flink-based version of the market-data processor.
-   A Kubernetes operator for worker deployments.

The main repository should keep one clear architecture. Avoid
implementing two versions of the same infrastructure component without a
concrete comparison goal.

------------------------------------------------------------------------

# 21. Portfolio demonstration and documentation

## 21.1 Recommended demo story

A concise demonstration should show the platform's integrated value:

1.  Open the dashboard and show a workflow definition.
2.  Start a market-data workflow.
3.  Show tasks being distributed to workers.
4.  Show events flowing through Kafka.
5.  Show aggregates appearing in TimescaleDB.
6.  Stop a worker or introduce database latency.
7.  Show retries, lease expiry, backpressure, or consumer lag.
8.  Show workflow recovery.
9.  Ask the AI copilot why the pipeline slowed or failed.
10. Show retrieved runbook evidence and live metrics.
11. Show a proposed recovery action.
12. Approve the action and show FlowForge recording the decision and
    execution.

## 21.2 README structure

Recommended README sections:

1.  Project summary
2.  Problem statement
3.  Feature list
4.  Architecture diagram
5.  Component responsibilities
6.  Technology stack
7.  Quick start
8.  Demo workflow
9.  Failure scenarios
10. Testing instructions
11. Observability screenshots
12. Performance methodology and measured results
13. Architecture decisions
14. Limitations
15. Roadmap

## 21.3 Visual artifacts to capture after implementation

The Markdown diagrams in this specification are source diagrams and
should render in Mermaid-compatible viewers such as GitHub.

After the system is running, capture real screenshots---not fabricated
mockups---of:

-   System dashboard
-   Workflow DAG execution
-   Worker status
-   Raft cluster leader/follower state
-   Kafka consumer lag
-   Market-data aggregate charts
-   Grafana metrics
-   AI investigation result with evidence
-   Approval and recovery flow

These screenshots can be stored under `docs/images/` and embedded in the
README. There are no product or facility photographs required for this
software platform; actual application screenshots and architecture
diagrams are more meaningful portfolio visuals.

## 21.4 Benchmark reporting

If reporting performance, document:

-   Hardware and operating system
-   Go version and dependency versions
-   Docker resource limits
-   Number of scheduler nodes
-   Number of workers
-   Number of Kafka partitions
-   Event volume and event size
-   Test duration
-   Warm-up period
-   Throughput definition
-   Latency percentile definitions
-   Failure/recovery conditions

Do not write claims such as "handles millions of events" or "highly
scalable" unless measured and reproducible.

------------------------------------------------------------------------

# 22. Resume positioning

Only use bullets that describe implemented and tested work. Do not put
planned features on a resume as completed accomplishments.

## 22.1 Potential resume bullets after implementation

**Workflow runtime**

-   Developed a Go-based distributed workflow runtime supporting DAG
    execution, durable task scheduling, worker coordination, retries,
    and recovery.

**Raft coordination**

-   Implemented a Raft-backed replicated key-value coordination service
    using `etcd/raft`, with scheduler leadership, worker leases,
    persistent state, and failure-injection tests.

**Streaming platform**

-   Built a Kafka-based market-data processing pipeline with Go
    consumers, event deduplication, backpressure, replay, and
    TimescaleDB aggregation.

**AI operations**

-   Integrated a LangChain/LangGraph RAG-based operations copilot that
    correlates workflow history, logs, metrics, and runbooks to generate
    evidence-backed incident diagnoses and recovery recommendations.

## 22.2 Add measured outcomes later

Potential measurements to collect:

-   Workflow task throughput
-   Median and p95 task scheduling latency
-   Worker failure recovery time
-   Scheduler failover time
-   Events processed per second
-   Consumer lag under load
-   Replay throughput
-   Duplicate-event correctness
-   Database write latency
-   AI investigation duration
-   Retrieval relevance and groundedness evaluation

Only include actual results, with test conditions. Do not invent
percentages or performance numbers.

## 22.3 How to explain the project in an interview

A strong explanation should follow this order:

1.  **Problem:** distributed jobs need durable execution, coordination,
    recovery, and visibility.
2.  **Design:** separate control plane, data plane, and AI operations
    plane.
3.  **Tradeoff:** Raft is for strongly coordinated metadata; Kafka is
    for durable event streaming.
4.  **Reliability:** workers can disappear, tasks can run more than
    once, and stale attempts must be fenced.
5.  **Streaming:** backpressure, replay, partition ordering,
    deduplication, and late events matter.
6.  **AI:** the agent uses tools and retrieved evidence; it does not
    guess or directly perform risky actions.
7.  **Proof:** show tests, failure demonstrations, metrics, and
    documented limitations.

------------------------------------------------------------------------

# 23. Risks and mitigations

  -----------------------------------------------------------------------
  Risk                    Why it matters          Mitigation
  ----------------------- ----------------------- -----------------------
  Scope becomes too large Too many technologies   Build in phases; make
                          can prevent a finished  the workflow engine
                          system                  work first

  Raft correctness is     Consensus bugs can      Use `etcd/raft`;
  difficult               invalidate coordination document its role; test
                                                  partitions and restarts

  Duplicate task          Distributed retries can Idempotency keys,
  execution               repeat side effects     attempt IDs, fencing
                                                  tokens

  Workflow DB and Raft    They are separate       Define ownership
  state diverge           persistence systems     boundaries and
                                                  reconciliation behavior

  Kafka offsets           Can cause data loss or  Commit after durable
  mishandled              repeated processing     processing; make writes
                                                  idempotent

  Replay corrupts newer   Backfills may overlap   Versioned/idempotent
  aggregates              live data               upserts and explicit
                                                  event-time rules

  Backpressure is only    Unbounded buffers hide  Bounded channels,
  cosmetic                overload                pause/resume,
                                                  queue-depth metrics

  AI invents root causes  LLMs can produce        Typed tools, evidence
                          plausible but           references,
                          unsupported claims      uncertainty, evaluation

  Duplicate orchestration FlowForge and LangGraph FlowForge is outer
                          could overlap           durable runtime;
                                                  LangGraph is internal
                                                  AI graph

  UI work consumes time   Frontend polish can     Build only the
                          delay core engineering  operational views
                                                  needed for the demo

  Benchmarks are          Performance claims      Publish methodology and
  misleading              depend on environment   raw results

  External data source    Public APIs can         Synthetic/historical
  changes                 rate-limit or change    adapter is the
                                                  deterministic baseline
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 24. Definition of done

The project is ready for a strong portfolio release when all of the
following are true.

## Core workflow engine

-   [ ] Workflow definitions can be created and versioned.
-   [ ] DAG validation rejects cycles and invalid dependencies.
-   [ ] Tasks execute across workers.
-   [ ] Workflow and task state is durable.
-   [ ] Retries and timeouts work.
-   [ ] Worker heartbeat and lease behavior work.
-   [ ] Stale attempts cannot overwrite newer results.
-   [ ] Scheduler restart recovers pending work.

## Raft coordination

-   [ ] Three-node cluster starts locally.
-   [ ] Replicated KV writes are applied consistently.
-   [ ] Leader failover works with quorum.
-   [ ] Writes are rejected without quorum.
-   [ ] Follower restart/catch-up works.
-   [ ] Snapshot/restore is tested.
-   [ ] Scheduler leadership uses an epoch/fencing mechanism.

## Streaming pipeline

-   [ ] Kafka producer and consumer work.
-   [ ] Events are partitioned by symbol.
-   [ ] Validation and deduplication work.
-   [ ] Aggregates are persisted in TimescaleDB.
-   [ ] Backpressure is bounded and observable.
-   [ ] Replay/backfill works.
-   [ ] Broker, consumer, and database failures are tested.

## AI Ops

-   [ ] Runbooks and docs are ingested.
-   [ ] Retrieval returns relevant chunks.
-   [ ] LangGraph can run a multi-step investigation.
-   [ ] Workflow and operational tools return real system data.
-   [ ] Results distinguish facts from hypotheses.
-   [ ] Evidence references are included.
-   [ ] Insufficient evidence is handled explicitly.
-   [ ] Risky actions require approval.

## Engineering quality

-   [ ] Unit tests pass.
-   [ ] Integration tests pass.
-   [ ] Fault tests are documented.
-   [ ] CI runs automatically.
-   [ ] Logs and traces include correlation IDs.
-   [ ] Metrics and dashboards are available.
-   [ ] Secrets are not committed.
-   [ ] README and diagrams are current.
-   [ ] Demo can be repeated from a clean environment.
-   [ ] All performance claims have measurement evidence.

------------------------------------------------------------------------

# 25. Recommended build order

For the most efficient path, follow this order:

1.  **Create the repository and design documents.**
2.  **Build a single-node Go workflow API and DAG validator.**
3.  **Persist workflow definitions, runs, tasks, and attempts in
    PostgreSQL.**
4.  **Build one scheduler and one worker.**
5.  **Add retries, timeouts, heartbeats, and restart recovery.**
6.  **Add end-to-end tests and a simple CLI or dashboard.**
7.  **Integrate the three-node Raft-backed KV service.**
8.  **Use Raft leadership and leases in scheduler/worker coordination.**
9.  **Add Kafka and a deterministic synthetic market-data producer.**
10. **Build the Go stream processor and TimescaleDB aggregation.**
11. **Add backpressure, consumer lag, and replay.**
12. **Integrate the market-data workload as a FlowForge DAG.**
13. **Add Python FastAPI AI service and pgvector knowledge base.**
14. **Build the LangGraph investigation workflow with read-only tools.**
15. **Add approval-gated recovery recommendations.**
16. **Instrument the platform with OpenTelemetry, Prometheus, and
    Grafana.**
17. **Run failure tests, load tests, and benchmark scenarios.**
18. **Polish README, diagrams, runbooks, screenshots, and demo.**

## Final recommendation

Build **FlowForge as one integrated platform**:

-   **Go** for the workflow runtime, scheduler, workers, coordination
    client, Raft-backed KV state machine, and stream processor.
-   **`etcd/raft`** for the consensus protocol, with FlowForge owning
    the KV state machine and integration.
-   **PostgreSQL** for durable workflow state.
-   **Kafka** for event streaming and replay.
-   **TimescaleDB** for market-data time-series aggregates.
-   **Python + FastAPI + LangChain + LangGraph + pgvector** for the AI
    Operations Copilot.
-   **React + TypeScript** for the operations dashboard.
-   **OpenTelemetry + Prometheus + Grafana** for observability.
-   **Docker Compose** for local development; Kubernetes as a later
    stretch goal.

The key is to finish the workflow runtime first, then add consensus,
streaming, and AI in layers. A smaller, tested, observable platform with
clear failure semantics is more impressive than a large collection of
partially connected technologies.

------------------------------------------------------------------------

# 26. Glossary

  -----------------------------------------------------------------------
  Term                                Meaning
  ----------------------------------- -----------------------------------
  DAG                                 Directed acyclic graph; tasks
                                      connected by dependency edges with
                                      no cycles

  Control plane                       Components that manage scheduling,
                                      configuration, and coordination

  Data plane                          Components that execute tasks and
                                      process data

  AI plane                            Components that perform AI-assisted
                                      investigation and recommendations

  Raft                                Consensus algorithm for replicated
                                      state and leader election

  Quorum                              Majority of voting members required
                                      for consensus progress

  State machine                       Deterministic component that
                                      applies committed commands in order

  Fencing token                       Monotonically increasing
                                      ownership/version value used to
                                      reject stale actors

  Lease                               Time-bounded ownership or
                                      registration record

  Kafka partition                     Ordered append-only subset of a
                                      Kafka topic

  Consumer group                      Consumers sharing work across topic
                                      partitions

  Consumer lag                        Difference between latest available
                                      records and a consumer's committed
                                      position

  Backpressure                        Mechanism that prevents a fast
                                      producer from overwhelming a slower
                                      consumer

  Replay                              Reprocessing retained events from a
                                      selected offset or time

  OHLCV                               Open, high, low, close, and volume
                                      aggregate

  VWAP                                Volume-weighted average price

  RAG                                 Retrieval-augmented generation;
                                      retrieving external knowledge to
                                      ground model responses

  LangChain                           Framework/integration layer for
                                      model, document, retriever, and
                                      tool integrations

  LangGraph                           Framework for stateful graph-based
                                      agent workflows

  pgvector                            PostgreSQL extension for vector
                                      similarity search

  OpenTelemetry                       APIs and SDKs for traces, metrics,
                                      and logs

  Idempotency                         Property where repeating an
                                      operation does not create
                                      additional unintended effects

  At-least-once                       Delivery/execution model where an
                                      operation may be repeated, so
                                      handlers must tolerate duplicates

  Dead-letter topic                   Kafka topic used to isolate events
                                      that cannot be processed normally

  ADR                                 Architecture Decision Record
                                      documenting a design choice and its
                                      tradeoffs
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## Notes on this specification

-   This document consolidates the FlowForge architecture and
    implementation recommendations discussed in the preceding
    conversation.
-   It is a design plan, not a claim that these components have already
    been implemented.
-   API names, database schemas, event fields, and repository paths are
    proposed starting points and can be refined during implementation.
-   The recommended path is deliberately scoped to produce a coherent,
    testable portfolio project before optional infrastructure is added.
