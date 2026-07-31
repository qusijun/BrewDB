# BrewDB Development Architecture

This document defines the Phase 1 development architecture for BrewDB. It translates the logical architecture into a crate/module structure for implementation. It does not define storage schemas, RPC wire formats, or Rust trait signatures.

## 1. Scope

Phase 1 implementation is organized as a monorepo with a small number of capability-oriented crates.

The crate layout is intentionally not based on deployment roles such as coordinator or worker. Binary packaging should also avoid turning those roles into the primary repository boundary.

This document now also fixes the top-down control flow for the real query mainline:

`brewdb -> brewdbd -> frontend -> sql -> planner -> runtime -> execution`

The key rule is that every layer must hand off one stable abstraction to the next layer rather than leaking local implementation details such as wire protocol fields, SQL parser details, or executor-specific task kinds.

## 1.1 Top-Level Binaries

### `brewdb`

`brewdb` is the SQL client.

It owns:

- CLI interaction
- remote session establishment
- SQL request submission
- client-side result rendering

It must not own:

- in-process runtime bootstrap
- direct runtime or execution calls
- coordinator-only planning shortcuts

### `brewdbd`

`brewdbd` is the server process host.

It owns:

- config and bootstrap
- listener lifecycle
- session ingress
- service assembly
- process-level observability

It must not own:

- SQL semantic planning
- distributed scheduling semantics
- execution fragment internals
- client wire protocol details outside ingress adapters

## 1.2 Top-Down Query Path

The primary query path should remain explicit:

1. `brewdb` sends SQL over PostgreSQL wire protocol
2. `brewdbd` accepts the connection and hands it to a protocol-specific `SessionIngress`
3. `brewdb-frontend` opens or resumes a client session and normalizes the SQL request
4. `brewdb-sql` and `brewdb-planner` build:
   - parsed statement
   - bound statement
   - logical plan
   - distributed plan
   - fragment-local execution plans
5. `brewdb-runtime` admits the distributed plan, projects fragments into stage templates, and drives scheduling
6. `brewdb-execution` runs worker task payloads and returns exchange outputs or terminal results

The system should not skip directly from client entry to runtime with ad hoc fields such as:

- raw SQL plus default database
- task kinds that encode query semantics in names
- profile enums that stand in for real execution structure

## 1.3 Core Runtime Contracts

The main cross-layer contracts should converge on the following objects.

### Client-facing contracts

- `ClientContext`
- `RequestContext`
- `ClientSqlRequest`
- `OpenClientSession`
- `OpenedClientSession`

`ClientContext` carries client session, connection, defaults, identity, and capability metadata. `RequestContext` carries one request's tracing and correlation metadata. They must remain distinct.

Context boundary note:

- `SessionContext` belongs with request and runtime context flow, not inside `BoundStatement`
- `QueryContext` and `JobContext` may carry session-derived execution context later, but their field sets should remain open for now

### Compiler and planning contracts

- `ParsedStatement`
- `BoundStatement`
- optimized `LogicalPlan`
- `DistributedPlan`
- `PlanFragment`
- fragment-local `ExecutionPlan`

### Runtime and execution contracts

- `StageTemplate`
- `TaskInstance`
- `TaskPayload`
- `ExchangeEdge`
- `ExecutableFragment`

The intended chain is:

`ClientSqlRequest -> optimized LogicalPlan -> DistributedPlan -> StageTemplate -> TaskInstance -> TaskPayload`

## 1.4 Plan Layers

The system must keep four layers separate.

### `SqlAst`

Represents what the user wrote.

### optimized `LogicalPlan`

Represents query semantics after DataFusion logical optimization.

### `DistributedPlan`

Represents BrewDB's distributed decomposition of the optimized logical plan across exchange boundaries.

### fragment-local `ExecutionPlan`

Represents the DataFusion physical execution tree for one fragment after fragment-local physical planning and physical optimization.

## 1.5 Fragment, Stage, and Task

These terms must not be mixed.

### `Fragment`

A distributed execution unit bounded by exchange semantics and backed by one local logical subplan.

### `Stage`

A runtime scheduling template derived one-to-one from a fragment.

### `Task`

One concrete partition attempt of one stage.

The intended expansion is:

`optimized LogicalPlan -> DistributedPlan -> StageTemplate -> TaskInstance -> TaskPayload`

`StageGraph` should not evolve as a second independent source of truth beside `DistributedPlan`. Runtime may project a lightweight stage view from `DistributedPlan`, but query semantics should continue to live in fragment structure plus exchange edges.

## 1.6 Core Type Sketches

The following sketches are not final Rust signatures. They define the intended shape and ownership of the main cross-layer contracts.

### Client and request entry

```rust
struct ClientContext {
    session: ClientSessionContext,
    connection: Option<ClientConnectionContext>,
    defaults: ClientDefaults,
    identity: ClientIdentity,
    capabilities: ClientCapabilities,
}

struct ClientSqlRequest {
    client_context: ClientContext,
    request_context: RequestContext,
    sql: String,
}
```

`ClientContext` is client-facing session truth. `RequestContext` is one-request correlation truth. Neither should absorb runtime job state.

### Query planning handoff

```rust
struct PlannedQuery {
    job_id: JobId,
    context: PlannedQueryContext,
    statement: QueryStatementInfo,
    logical_plan: LogicalPlanHandle,
    distributed_plan: DistributedPlan,
    result_schema: TableSchema,
    diagnostics: QueryPlanDiagnostics,
}

struct PlannedQueryContext {
    client: ClientContext,
    request: RequestContext,
}

struct QueryStatementInfo {
    sql_text: String,
    statement_class: StatementClass,
    reads: Vec<LogicalTableName>,
}
```

Rules:

- `PlannedQuery` is the formal compiler-to-runtime query handoff
- runtime scheduling decisions depend on `distributed_plan`, not on `sql_text`
- DataFusion logical optimization runs before `DistributedPlanner`
- DataFusion physical planning and physical optimization run per fragment after `DistributedPlanner`

### Distributed plan

```rust
struct DistributedPlan {
    fragments: Vec<PlanFragment>,
    output_fragment_id: FragmentId,
}

struct PlanFragment {
    fragment_id: FragmentId,
    logical_plan: LogicalPlanHandle,
    input: FragmentInputSpec,
    output: FragmentOutputSpec,
    parallelism: FragmentParallelism,
    placement: FragmentPlacement,
    upstreams: Vec<FragmentInputEdge>,
}

struct FragmentInputEdge {
    upstream_fragment_id: FragmentId,
    exchange: ExchangeEdge,
}
```

```rust
enum FragmentInputSpec {
    Source,
    Exchange,
    Mixed,
}

enum FragmentOutputSpec {
    Exchange,
    Result,
}

enum FragmentParallelism {
    Fixed(u32),
    FollowSourcePartitions,
    Singleton,
}

enum FragmentPlacement {
    Anywhere,
    CoordinatorPreferred,
    WorkerOnly,
}
```

Rules:

- a fragment is a distributed execution unit bounded by exchange semantics
- fragment boundaries come from exchange semantics, not from SQL statement class names
- one fragment becomes one runtime stage template

### Exchange edge

```rust
struct ExchangeEdge {
    exchange_id: ExchangeId,
    distribution: ExchangeDistribution,
    ordering: ExchangeOrdering,
}

enum ExchangeDistribution {
    Partitioned { keys: Vec<ExchangeKey> },
    Broadcast,
    Gather,
    Passthrough,
}

enum ExchangeOrdering {
    Unspecified,
    Preserved,
}
```

Rules:

- `ExchangeEdge` is the fragment-to-fragment data movement contract
- exchange semantics must be explicit in structure rather than implied by stage names

### Fragment-local executable

```rust
struct ExecutableFragment {
    backend: ExecutionBackend,
    descriptor: FragmentDescriptor,
}

enum FragmentDescriptor {
    Opaque {
        format: FragmentFormat,
        bytes: Vec<u8>,
    },
}
```

Rules:

- the top-level architecture should not leak `DataFusion...` types in its main contracts
- the descriptor must be transportable and recoverable
- backend-local registry or handle shortcuts are allowed as implementation optimizations, not as the architecture boundary
- `FragmentPhysicalPlanner` is responsible for turning `PlanFragment.logical_plan` into a fragment-local DataFusion `ExecutionPlan`

### Runtime stage and task projection

```rust
struct StageTemplate {
    stage_id: StageId,
    fragment_id: FragmentId,
    parallelism: u32,
    placement: FragmentPlacement,
    input_mode: StageInputMode,
    output_mode: StageOutputMode,
    dependencies: Vec<StageDependency>,
}

struct StageDependency {
    upstream_stage_id: StageId,
    exchange_id: ExchangeId,
}
```

```rust
enum StageInputMode {
    Source,
    Exchange,
    Mixed,
}

enum StageOutputMode {
    Exchange,
    Result,
}
```

Rules:

- runtime projects `StageTemplate` from `DistributedPlan`
- `StageTemplate` is a scheduling view, not a second semantic execution graph
- `StageGraph` should remain a projection or convenience view only

### Task instance and worker payload

```rust
struct TaskInstance {
    task_id: TaskId,
    stage_id: StageId,
    fragment_id: FragmentId,
    partition_id: u32,
    attempt: u32,
}

struct TaskPayload {
    task_id: TaskId,
    stage_id: StageId,
    fragment_id: FragmentId,
    partition_id: u32,
    attempt: u32,
    executable: ExecutableFragment,
    inputs: Vec<TaskInput>,
    output: TaskOutput,
}
```

```rust
enum TaskInput {
    SourcePartition {
        partition_id: u32,
    },
    ExchangeInput {
        exchange_id: ExchangeId,
        partitions: Vec<ExchangePartitionRef>,
    },
}

enum TaskOutput {
    ExchangeSink {
        exchange_id: ExchangeId,
    },
    ResultSink,
}
```

Rules:

- `TaskInstance` is runtime state
- `TaskPayload` is worker-facing execution input
- workers should receive structured payloads rather than SQL text plus profile hints

## 1.6.1 Frontend Session and Statement Routing

The frontend boundary should decide whether a statement stays inside session handling or enters the planning and runtime path.

Recommended split:

- session-local statements stay in `brewdb-frontend`
- runtime-bound statements enter `brewdb-sql` and the planner stack

Session-local statements typically include:

- `SET`
- `PREPARE`
- `EXECUTE`
- transaction control statements handled as session/runtime coordination entry
- protocol or client-local statement flow that does not require distributed execution

Runtime-bound statements typically include:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`
- DDL statements that require catalog truth changes
- maintenance statements that require distributed planning or storage interaction

Frontend boundary rule:

- `brewdb-frontend` owns session state and statement routing
- `brewdb-sql` owns statement parsing, classification, and SQL-facing rewrites
- `brewdb-planner` owns planning entry into optimized logical plans and distributed planning
- only runtime-bound statements should enter the `DistributedPlanner` path

## 1.6.2 SQL To Planner Handoff

`brewdb-sql` and `brewdb-planner` should meet on one explicit handoff object rather than ad hoc parameter lists.

Recommended layering:

- `ParsedStatement`
- `BoundStatement`
- `StatementFamily`
- `PlannerRequest`
- `PlannerOutput`

Type sketch:

```rust
struct PlannerRequest {
    client: ClientContext,
    request: RequestContext,
    statement: BoundStatement,
    statement_family: StatementFamily,
    catalog_snapshot: PlannerCatalogSnapshot,
    planning_options: PlanningOptions,
}

struct PlannerOutput {
    statement: BoundStatement,
    optimized_logical_plan: LogicalPlanHandle,
    distributed_plan: DistributedPlan,
    result_schema: TableSchema,
    diagnostics: PlannerDiagnostics,
}
```

Rules:

- `brewdb-sql` stops at bound statement plus planner handoff assembly
- `brewdb-planner` starts from `PlannerRequest`, not from raw SQL text
- `BoundStatement` is a binder output, not a session container
- `BoundStatement` may carry only the minimal `SessionSemantics` snapshot needed to freeze SQL meaning before planning
- full `SessionContext` should stay with request and runtime context flow such as `ClientContext`, future `QueryContext`, and future `JobContext`
- `PlannerRequest` may carry planning-visible catalog snapshot data, but should not embed runtime job state
- `PlannerOutput` is the planner-to-runtime boundary for runtime-bound statements
- runtime scheduling depends on `DistributedPlan`, while client result shaping may additionally depend on `result_schema` and `diagnostics`

`StatementFamily` should stay coarse and routing-oriented.

Recommended Phase 1 families:

- `Query`
- `Insert`
- `Update`
- `Delete`
- `Merge`
- `Ddl`
- `Maintenance`

Boundary rule:

- statement family exists to route planner and runtime entry policy
- operator-level execution shape still comes from optimized logical plan plus distributed planning, not from statement-family-specific runtime code paths

## 1.7 Planner Layering

The planner stack should be split into three distinct phases.

1. DataFusion SQL parsing and binding:
   - `Statement`
   - relation and name resolution
2. DataFusion logical optimization:
   - global logical rewrites
   - expression simplification
   - predicate and projection pushdown
3. BrewDB distributed planning:
   - `DistributedPlanner` consumes the optimized `LogicalPlan`
   - produces `DistributedPlan`
   - owns distributed CBO
   - decides exchange topology, fragment boundaries, parallelism, and placement
4. Fragment-local physical planning:
   - `FragmentPhysicalPlanner` consumes each `PlanFragment.logical_plan`
   - invokes DataFusion physical planner and physical optimizer
   - produces fragment-local `ExecutionPlan`

This means BrewDB does not build a second SQL parser or a second general-purpose logical optimizer.
It does own distributed planning and distributed CBO after DataFusion logical optimization and before fragment-local physical planning.

### `DistributedPlanner` internal responsibilities

`DistributedPlanner` should stay focused on distributed execution shape rather than local operator implementation details.

Its main internal responsibilities are:

1. boundary detection
   - identify where the optimized logical plan must be split into multiple distributed fragments
   - detect exchange-requiring transitions such as gather, repartition, multi-phase aggregation, and mutation publish boundaries
2. fragment cutting
   - carve one optimized global logical plan into multiple local logical subplans
   - assign each local logical subplan to one `PlanFragment`
3. exchange planning
   - create `ExchangeEdge` links between fragments
   - decide exchange distribution mode such as gather, partitioned, broadcast, or passthrough
4. parallelism and placement policy
   - choose fragment parallelism
   - choose fragment placement such as coordinator-preferred or worker-only
5. distributed cost-based optimization
   - consume distributed planning statistics
   - choose among competing distributed shapes such as broadcast versus repartition and one-stage versus multi-stage aggregation

`DistributedPlanner` must not own:

- DataFusion local physical operator generation
- fragment-local physical optimizer execution
- runtime task scheduling state
- storage-format-native commit truth

## 2. Crate Layout

Phase 1 uses eight primary crates.

### `brewdb-common`

Shared foundation components:

- logging bootstrap
- diagnostics and error codes
- base error helpers
- job-config layering primitives
- process-global tracing subscriber policy, including upstream DataFusion target collection
- structured event helpers for stable event target fields, `event_name`, `error_code`, and `job_id`
- future low-level config and utility components that are genuinely cross-crate

Job-config precedence is fixed as:

- `system < session < statement`
- all job-config keys must use the `brewdb.` prefix
- the registry is the whitelist source of truth for all legal job-config keys, defaults, types, and allowed scopes

### `brewdb-catalog`

Catalog metadata kernel:

- BrewDB-owned catalog service
- FoundationDB-backed catalog-store access
- cache
- normalization
- catalog/database/table resolution
- table storage binding resolution

### `brewdb-sql`

Language frontend:

- parsing
- binding
- statement analysis
- SQL rewrites
- statement classification
- planner handoff assembly
- SQL-surface capability gating

### `brewdb-planner`

Planning kernel:

- planning entry for runtime-bound statements
- DataFusion logical optimization bridge
- distributed planning
- distributed CBO
- fragment-local physical planning entry
- `DistributedPlan` assembly

### `brewdb-frontend`

External SQL ingress:

- protocol-neutral client session and request handling
- session and authentication entry
- statement routing at the frontend boundary
- planner/runtime handoff for runtime-bound statements
- result shaping and encoding back to clients
- protocol-specific ingress adapters such as PostgreSQL wire protocol
- protocol-facing error mapping

### `brewdb-execution`

Distributed execution kernel:

- execution backend integration
- executable fragment materialization
- worker task request/result contract
- worker runtime shared logic
- exchange buffer and sink/source behavior
- materialization contracts
- execution-side data cache
- Arrow as the in-memory execution format baseline

Execution-format hard constraint:

- inside `brewdb-execution`, operator state, task handoff, and exchange payloads stay Arrow-compatible
- BrewDB does not define a second private execution row format alongside DataFusion
- any row-oriented or protocol-oriented re-encoding belongs above execution, such as coordinator result shaping or `pgwire` response encoding

### `brewdb-storage`

Storage semantics kernel:

- storage engine entry
- per-table storage engines
- scan/append/rewrite/maintenance/commit semantics
- format truth interpretation
- concrete format implementations such as Paimon and Iceberg

### `brewdb-runtime`

Lifecycle and control kernel:

- job orchestration
- transaction orchestration
- fragment scheduling
- transaction coordination
- lease handling
- recovery
- mutation orchestration
- maintenance orchestration
- fragment-graph to stage-template projection
- scheduler and dispatcher
- task payload assembly

## 2.1 Crate Responsibility Matrix

To keep the eight crates from drifting into each other, each crate should be judged by four questions:

- what truth does it own
- what inputs it consumes
- what outputs it produces
- what it must not own

### `brewdb-common`

Owns:

- logging bootstrap and subscriber wiring
- shared diagnostics and error-code vocabulary
- low-level common helpers reused across crates

Consumes:

- no BrewDB crate

Produces:

- stable foundational components reused by all upper layers

Must not own:

- orchestration
- transport
- query/catalog/runtime domain ownership
- control-plane IO

### `brewdb-catalog`

Owns:

- catalog access service
- normalized catalog objects
- catalog cache and route resolution
- catalog-store persistence boundary

Consumes:

- `brewdb-common`
- FoundationDB client and catalog-store integrations

Produces:

- normalized catalog/database/table metadata
- storage binding lookup results
- format-routing information

Must not own:

- job lifecycle
- transaction truth
- final format commit semantics
- execution planning

### `brewdb-sql`

Owns:

- SQL parsing and binding
- statement analysis
- statement classification
- planner handoff assembly
- SQL-surface capability checks

Consumes:

- `brewdb-common`
- `brewdb-catalog`

Produces:

- statement-family outputs
- parsed statements
- bound statements

Must not own:

- table-engine-native semantics
- runtime-state persistence
- runtime scheduling state

### `brewdb-planner`

Owns:

- planning entry for runtime-bound statements
- DataFusion logical optimization bridge
- distributed planning and distributed CBO
- fragment-local physical planning entry
- `DistributedPlan` and fragment planning artifacts

Consumes:

- `brewdb-common`
- `brewdb-catalog`
- `brewdb-sql`
- `brewdb-storage`
- `brewdb-execution`

Produces:

- optimized logical plans
- `DistributedPlan`
- fragment-local execution plan artifacts
- planner diagnostics used by runtime and result shaping

Must not own:

- client session truth
- runtime-state persistence
- worker scheduling state
- format-native publish truth

### `brewdb-frontend`

Owns:

- protocol-neutral client session handling
- SQL session entry and statement flow
- statement routing between session-local and runtime-bound paths
- protocol adapter entry such as pgwire translation
- result-shape encoding toward clients

Consumes:

- `brewdb-common`
- `brewdb-sql`
- `brewdb-runtime`

Produces:

- protocol-neutral request handoff into BrewDB internals
- planner/runtime handoff for runtime-bound statements
- protocol-specific response frames back to clients

Must not own:

- SQL semantic planning
- distributed scheduling
- storage semantics
- transaction truth

### `brewdb-execution`

Owns:

- execution backend-specific full-plan handling
- executable fragment materialization
- task request/result contract
- materialization and boundary output contracts
- executor runtime behavior
- execution-graph boundary semantics
- Arrow-native in-memory batch and stream contracts for execution

Consumes:

- `brewdb-common`
- selective execution-facing requirements from runtime and storage

Produces:

- executable fragments
- task payloads
- task results
- staged artifact references
- boundary summaries

Must not own:

- job ownership
- table leases
- transaction state
- commit journaling
- catalog mutation
- client-facing wire result encoding

### `brewdb-storage`

Owns:

- storage engine boundary
- per-table storage engine boundary
- scan/append/rewrite/maintenance/commit semantics
- format truth interpretation
- reconciliation truth lookup

Consumes:

- `brewdb-common`
- `brewdb-catalog`
- selective `brewdb-execution` contracts for artifact and scan shape requirements

Produces:

- capability views
- scan and materialization requirements
- commit validation and publish behavior
- reconciliation behavior

Must not own:

- SQL parsing
- runtime orchestration
- distributed scheduling
- catalog-store backend ownership outside its own crate boundary

## 2.5.1 Storage Engine Framework

The storage layer should be modeled around three levels:

- `StorageEngine`
- `TableEngine`

Intended chain:

`planner / FragmentScheduler / TxnCoordinator -> StorageEngine -> TableEngine`

Role split:

- `StorageEngine`
  - the top-level storage entry for BrewDB
  - opens one `TableCatalogEntry` into one `TableEngine`
  - routes by table format and storage binding
- `TableEngine`
  - the storage-side capability object for one resolved table
  - owns that table's scan, append, rewrite, statistics, and publish behavior

Concrete implementations:

- `PaimonTableEngine`
- `IcebergTableEngine`

Framework rules:

- `StorageEngine` does not own catalog truth
- `StorageEngine` does not own transaction truth
- callers should not branch on format directly once they hold a `TableEngine`
- format-native metadata interpretation stays inside concrete `TableEngine` implementations
- `brewdb-catalog` may expose `TableFormat` for routing, but must not expose format-native internal metadata models

### `brewdb-runtime`

Owns:

- job lifecycle
- transaction lifecycle
- transaction management locking
- fragment scheduling shell
- transaction coordination shell
- lease and recovery framework
- mutation and maintenance orchestration
- scheduler policy and dispatch coordination
- stage-template projection
- task instance lifecycle

Consumes:

- `brewdb-common`
- `brewdb-catalog`
- `brewdb-execution`
- `brewdb-storage`

Produces:

- orchestration plans
- dispatch decisions
- runtime-state transitions
- commit attempts
- recovery actions
- stage templates
- task instances
- worker assignments

Must not own:

- format-native truth
- task execution internals
- raw control-plane client details
- SQL syntax concerns
- external client wire protocols

## 2.2 Crate Collaboration Paths

The main allowed call paths should stay narrow:

1. SQL ingress path:
   `brewdbd ingress -> brewdb-frontend -> brewdb-sql -> brewdb-planner -> brewdb-runtime -> brewdb-execution / brewdb-storage`
2. Commit path:
   `brewdb-runtime -> brewdb-catalog -> brewdb-storage`
3. Execution path:
   `brewdb-runtime -> brewdb-execution`
4. Recovery path:
   `brewdb-runtime -> brewdb-catalog + brewdb-storage`

The main disallowed shortcuts are:

- `brewdb-sql -> brewdb-storage`
- `brewdb-frontend -> brewdb-storage`
- `brewdb-frontend -> brewdb-execution`
- `brewdb-sql -> brewdb-runtime`
- `brewdb-execution -> brewdb-runtime`
- `brewdb-execution -> brewdb-catalog`
- `brewdb-storage -> brewdb-runtime`
- `brewdbd` top-level server code reaching into runtime or execution internals directly
- upper layers depending on raw catalog-store record layouts or backend-specific response types

## 2.3 Crate Public Surface

Each crate should expose a small, intentional top-level surface. Public API design should optimize for stable collaboration boundaries, not convenience re-export sprawl.

### `brewdb-common`

Recommended public surface:

- `logging`
- `diagnostics`
- `errors`
- `config`

Recommended internal-only details:

- subscriber construction helpers that are purely crate-local
- serialization glue that exists only for one caller
- test-only builders and fixtures

Public API rule:

- `brewdb-common` should expose only foundational components and must not become a second home for catalog/runtime/execution domain models

### `brewdb-catalog`

Recommended public surface:

- `service`
- `model`
- `path`
- `backend`
- `errors`

Recommended internal-only details:

- `client`
- `cache`
- `normalize`
- backend-specific catalog-store modules

Public API rule:

- callers should depend on catalog-facing models and `CatalogService` entry points, never on raw catalog-store backend types

### `brewdb-sql`

Recommended public surface:

- `ast`
- `bind`
- `analyze`
- `rewrite`
- `statement`
- `capabilities`
- `errors`

Recommended internal-only details:

- parser implementation glue
- tokenization details
- frontend-only normalization helpers

Public API rule:

- the most important stable outputs are bound statement objects and planner handoff objects, not parser internals

### `brewdb-planner`

Recommended public surface:

- `logical`
- `distributed`
- `physical`
- `diagnostics`
- `errors`

Recommended internal-only details:

- DataFusion planning glue
- local planning skeletons used only inside `DistributedPlanner`
- planner-side statistics plumbing

Public API rule:

- upper layers should depend on planner outputs such as optimized logical plans and `DistributedPlan`, not on DataFusion integration glue

### `brewdb-frontend`

Recommended public surface:

- `pgwire`
- `session`
- `auth`
- `portal`
- `result`
- `errors`

Recommended internal-only details:

- protocol parser internals
- frame codec helpers
- transport-server bindings

Public API rule:

- upper layers should see session and request handoff contracts, not raw socket or codec details

### `brewdb-execution`

Recommended public surface:

- `plan`
- `task`
- `boundaries`
- `artifacts`
- `errors`
- Arrow-facing batch/stream contracts once they become explicit types

Conditionally public surface:

- `runtime` only for clearly shared runtime contracts

Recommended internal-only details:

- executor implementation internals
- local scheduling details
- spill/shuffle implementation specifics
- worker-local service wiring
- metrics backend bindings

Public API rule:

- upper layers may see execution contracts, but should not bind to executor implementation structure
- when execution data crosses a task or stage runtime boundary, the baseline in-memory shape is Arrow-compatible columnar data rather than a BrewDB-private row format

Exchange transport rule:

- exchange logical data stays Arrow-native at the execution contract level
- local in-process exchange should prefer direct Arrow batch or stream handoff rather than forced serialization
- remote cross-process or cross-node exchange should use Arrow IPC stream as the default wire format
- BrewDB may define exchange control metadata such as stage, task, partition, sequence, and end-of-stream markers, but should not define a second private data payload format alongside Arrow

### `brewdb-storage`

Recommended public surface:

- `engine`
- `model`
- `route`
- `errors`

Conditionally public surface:

- `scan`
- `append`
- `rewrite`
- `maintenance`
- `commit`

These operation modules may remain public if they define stable engine-facing contracts. If they become implementation-heavy, they should collapse behind `engine`.

Recommended internal-only details:

- format-specific helper modules
- metadata parsing internals
- format-native reconciliation subroutines
- per-format optimization heuristics

Public API rule:

- callers should depend on `StorageEngine` and `TableEngine` contracts, not on per-format implementation modules

### `brewdb-runtime`

Recommended public surface:

- `scheduler`
- `txns`
- `locks`
- `recovery`
- `runtime_meta`
- `errors`

Conditionally public surface:

- `mutation`
- `maintenance`
- `runtime`

Recommended internal-only details:

- orchestration step executors
- persistence backends
- retry loops
- background housekeeping internals

Public API rule:

- other crates should depend on orchestration entry points and shared lifecycle models, not on step-by-step coordinator internals

## 2.4 Re-export Policy

To prevent public API drift, the workspace should follow a strict re-export policy.

Allowed:

- re-exporting small, stable domain types that improve ergonomics
- re-exporting crate-local service entry points
- re-exporting intentionally stable contract modules

Avoid:

- wildcard re-exports across major submodules
- re-exporting implementation helpers only because another crate currently uses them
- exposing vendor-specific or transport-specific types at the crate root

Default rule:

- if a module is not part of a crate's architecture boundary, do not make it part of the public prelude

## 2.5 Main Type Ownership

The architecture should also be explicit about where the main system objects live. If a type is shared across crate boundaries, its home crate should be chosen by ownership truth, not convenience.

### `brewdb-common`

Should own the canonical type definitions for:

- stable error-code enums
- logging configuration primitives
- tracing bootstrap options

Rule:

- if a type is a low-level foundational primitive reused broadly and does not imply domain ownership, it belongs in `brewdb-common`

### `brewdb-catalog`

Should own:

- `CatalogPath`
- `DatabasePath`
- `TablePath`
- `CatalogRef`
- `DatabaseRef`
- `TableRef`
- `CatalogEntry`
- `DatabaseEntry`
- `TableCatalogEntry`
- `TableStorageSpec`

Rule:

- if a type represents normalized catalog truth or stable catalog routing inputs, it belongs in `brewdb-catalog`

### `brewdb-sql`

Should own:

- AST types
- bound statement types
- statement-family and statement-routing types
- frontend capability diagnostics

Rule:

- if a type is meaningful only before orchestration handoff, it belongs in `brewdb-sql`

### `brewdb-planner`

Should own:

- optimized logical plan outputs
- `DistributedPlan`
- `PlanFragment`
- planner diagnostics
- fragment-local physical planning artifacts

Rule:

- if a type is meaningful only after SQL classification and before runtime scheduling, it belongs in `brewdb-planner`

### `brewdb-execution`

Should own:

- projected `StageGraph` helpers when needed
- `StagePlan`
- `TaskRequest`
- `TaskResult`
- `BoundaryKind`
- `MaterializationContract`
- `SelectionResult`
- execution-facing artifact summary types

Rule:

- if a type describes execution slicing, task dispatch, or boundary outputs, it belongs in `brewdb-execution`

### `brewdb-storage`

Should own:

- `StorageEngine`
- `TableEngine`
- scan requirement types
- append/rewrite realization types
- commit validation request/result types
- reconciliation request/result types
- format capability views

Rule:

- if a type expresses format-aware semantics or storage-engine contracts, it belongs in `brewdb-storage`

### `brewdb-runtime`

Should own:

- `JobRecord`
- `TxnRecord`
- `CommitAttemptRecord`
- `ResourceLeaseRecord`
- runtime orchestration plan types
- recovery work item types
- dispatch decision types

Rule:

- if a type represents runtime truth, orchestration truth, or recovery truth, it belongs in `brewdb-runtime`

## 2.6 Runtime Metadata Boundary

Runtime metadata should be modeled as a first-class framework boundary, not just a persistence detail.

At the system level, BrewDB has two metadata cores:

- `CatalogMeta`
- `RuntimeMeta`

They are both authoritative metadata subsystems, but they own different truths.

- `CatalogMeta` owns directory-style metadata:
  - catalog, database, and table identity
  - schema
  - format and storage binding
  - DDL object truth
- `RuntimeMeta` owns state-style metadata:
  - job lifecycle
  - txn lifecycle
  - lease and ownership state
  - execution progress
  - commit attempts and recovery truth

Phase 1 backend choice for both subsystems is FoundationDB, but they must remain separated by:

- service boundary
- store boundary
- backend trait boundary
- keyspace boundary
- transaction semantic boundary

The runtime store should be owned by `brewdb-runtime`, but its records should connect cleanly to the rest of the workspace.

### Runtime-store families

Lifecycle records:

- `JobRecord`
- `StageRecord`
- `TaskAttemptRecord`

Transaction records:

- `TxnRecord`
- `CommitAttemptRecord`
- `TxnLockRecord`
- `ReconciliationRecord`

Coordination records:

- `JobOwnerRecord`
- `ResourceLeaseRecord`
- `ClusterLeaseRecord`

Artifact records:

- `ArtifactManifest`
- `ArtifactBundleRecord`

Design rules:

- `brewdb-runtime` owns write authority for runtime truth
- `brewdb-execution` may emit task facts, but should not own authoritative runtime transitions
- `brewdb-storage` may return validation/publish truth, but should not persist runtime orchestration truth directly
- `brewdb-catalog` remains outside runtime metadata ownership even when both logical stores use FoundationDB

## 2.6.1 RuntimeMeta Service Layering

`RuntimeMeta` should follow the same high-level service/store/backend split as catalog, but with state-oriented rather than directory-oriented semantics.

Recommended layers:

- `RuntimeMetaService`
- `RuntimeMetaStore`
- `RuntimeMetaBackend`

Intended chain:

`brewdb-runtime orchestration -> RuntimeMetaService -> RuntimeMetaStore -> RuntimeMetaBackend -> FoundationDB`

Role split:

- `RuntimeMetaService`
  - owns lifecycle actions and state transitions
  - examples:
    - create job
    - claim owner
    - register stages
    - append task attempt
    - transition txn state
    - record commit attempt
    - mark execution complete
- `RuntimeMetaStore`
  - owns record persistence and secondary-index maintenance
  - exposes read and write transactions over runtime records
- `RuntimeMetaBackend`
  - owns backend KV and transaction mechanics
  - hides FoundationDB transaction and keyspace details from upper runtime layers

Design rules:

- `RuntimeMetaService` is the only authoritative writer of runtime truth
- scheduler, dispatcher, and finalization code should not write backend records directly
- `RuntimeMetaStore` should expose runtime-domain records, not raw backend key-value shapes
- `RuntimeMetaBackend` should stay infrastructure-only and must not acquire orchestration semantics

## 2.6.1.1 Runtime Top-Level Framework

At the runtime framework level, Phase 1 should converge on three primary runtime services:

- `FragmentScheduler`
- `TxnCoordinator`
- `RuntimeMetaService`

Role split:

- `FragmentScheduler`
  - accepts a `DistributedPlan`
  - performs graph registration
  - projects fragments into stages and tasks
  - drives dispatch, progress, and stage completion
- `TxnCoordinator`
  - owns commit-bearing operation convergence
  - creates txn shells when needed
  - coordinates commit attempts, publish, and terminal txn resolution
  - owns recovery-oriented txn outcome convergence
- `RuntimeMetaService`
  - owns authoritative runtime truth
  - is the only runtime metadata writer seen by scheduler and transaction coordination flows

Boundary rule:

- `FragmentScheduler` owns distributed execution progress up to durable execution completion
- `TxnCoordinator` begins only when an operation requires commit-bearing convergence or recovery-oriented transaction resolution
- pure query operations should normally terminate inside `FragmentScheduler` without entering `TxnCoordinator`
- commit-bearing mutation, maintenance, or DDL flows may pass through both components

Naming rule:

- admission is a phase inside runtime entry and graph registration, not a required top-level service name
- finalization is a phase inside transaction coordination, not a required top-level service name

## 2.6.2 RuntimeMeta Integration Points

`RuntimeMeta` should connect to the execution chain at four explicit phases.

### Runtime entry phase

Before execution begins:

- allocate job identity
- persist initial `JobRecord`
- create txn shell when the operation is commit-bearing
- establish ownership and initial lease state

Primary owner:

- `FragmentScheduler` for execution entry
- `TxnCoordinator` when txn-bearing initialization is required

### Graph registration phase

After `DistributedPlan` is produced and before dispatch starts:

- register stage or fragment execution records
- persist initial execution-progress scaffolding
- make graph-wide admission visible to recovery

Primary owner:

- `FragmentScheduler`

### Execution-progress phase

While workers execute:

- record task dispatch
- append task-attempt facts
- mark stage completion
- persist exchange or artifact progress summaries when required

This is the first runtime-meta contact point that is truly in the hot execution path. It should therefore stay structured and incremental rather than trying to persist every executor-local detail.

Primary owner:

- `FragmentScheduler`

### Transaction convergence phase

After execution is done:

- transition job state into commit/finalization paths when applicable
- persist commit attempts
- converge txn state
- mark the job terminal only after finalization truth is durable

Primary owner:

- `TxnCoordinator`

Framework rule:

- `RuntimeMeta` should be touched before execution, during execution progress, and after execution, but only through `RuntimeMetaService`

## 2.6.3 Shared Schema and Type System

BrewDB should use one shared schema language across catalog, planning, execution, and runtime metadata references.

Recommended baseline:

- shared schema types live in `brewdb-common::schema`
- schema and column typing are Arrow-aligned
- BrewDB does not define a second independent execution type algebra beside Arrow/DataFusion

Shared core objects:

- `TableSchema`
- `ColumnSchema`
- Arrow-aligned data type representation

Ownership split:

- `brewdb-catalog` stores table schema truth inside `TableCatalogEntry`
- planning layers consume shared schema objects when building DataFusion plans
- `brewdb-execution` runs on Arrow arrays and `RecordBatch`
- `brewdb-runtime` may reference schema summaries, but should not invent a second schema model

Design rules:

- catalog and planner must not drift into separate schema vocabularies
- execution-format alignment should remain Arrow-first
- any thin BrewDB wrapper over Arrow data types must remain one-to-one aligned rather than becoming a second type system

## 2.7 Main Cross-Crate Framework Flows

The architecture should keep a few primary end-to-end flows explicit so crate boundaries can be judged against real movement, not only static responsibility lists.

### Session-local statement flow

1. `brewdb-frontend` accepts one SQL statement under one client session
2. `brewdb-sql` parses and classifies the statement
3. `brewdb-frontend` keeps session-local statements inside the session path
4. session state or protocol-local result shaping completes without entering distributed planning or execution

Framework rule:

- statement routing belongs at the frontend/session boundary before runtime-bound planning starts

### Query flow

1. `brewdb-sql` parses, binds, classifies, and produces planner input
2. `brewdb-planner` runs DataFusion logical optimization and builds `DistributedPlan`
3. `brewdb-catalog` resolves `TableCatalogEntry` objects and planning-visible metadata
4. `FragmentScheduler` accepts the operation into runtime and initializes runtime metadata
5. graph registration becomes visible through `RuntimeMetaService`
6. `FragmentScheduler` shapes orchestration and dispatch requirements
7. `brewdb-execution` runs tasks and reports execution progress through runtime-owned truth
8. results return through runtime-owned job truth, without creating txn/commit state

### Append flow

1. `brewdb-sql` parses, binds, classifies, and produces planner input for `INSERT`
2. `brewdb-catalog` resolves the target `TableCatalogEntry`
3. `brewdb-storage` returns append requirements
4. `brewdb-planner` produces the distributed plan and fragment-local planning artifacts
5. `FragmentScheduler` and `TxnCoordinator` create the job and txn shell through `RuntimeMetaService`
6. execution truth is registered before dispatch
7. `brewdb-execution` materializes staged append artifacts
8. `TxnCoordinator` acquires resource lane and txn lock
9. `brewdb-storage` validates and publishes the final append
10. `TxnCoordinator` resolves commit truth and cleanup eligibility

### Rewrite mutation flow

1. `brewdb-sql` parses, binds, classifies, and produces planner input for rewrite mutation
2. `brewdb-catalog` resolves the target `TableCatalogEntry`
3. `brewdb-storage` returns rewrite realization requirements
4. `brewdb-planner` produces the distributed plan and fragment-local planning artifacts
5. `FragmentScheduler` and `TxnCoordinator` establish the job, txn shell, and critical-section timing through `RuntimeMetaService`
6. execution truth is registered before dispatch
7. `brewdb-execution` scans, matches, and materializes staged mutation artifacts
8. `TxnCoordinator` enters the mutation lane, drives commit attempts, and coordinates publish
9. `brewdb-storage` validates and publishes format-native mutation results
10. `TxnCoordinator` resolves txn and artifact lifecycle truth

### Recovery flow

1. `brewdb-runtime` detects ownership loss or unknown outcome
2. `brewdb-runtime` loads runtime truth from the runtime store
3. `brewdb-catalog` reloads current table route and control-plane bindings
4. `brewdb-storage` resolves external format truth
5. `brewdb-runtime` finalizes txn resolution, lease cleanup, and artifact cleanup eligibility

Framework rule:

- every major user-visible operation should map onto one of these cross-crate flow shapes or a close variation of them

## 2.8 Runtime State Machine Matrix

The runtime framework should also freeze the coupling between its main state objects. Phase 1 should not let each subsystem invent its own interpretation of job, transaction, commit, and lock progression.

### Main runtime objects

- `JobRecord`
- `TxnRecord`
- `CommitAttemptRecord`
- `TxnLockRecord`

### State-object roles

`JobRecord`:

- user-visible lifecycle truth for one admitted operation

`TxnRecord`:

- transaction lifecycle truth for one commit-bearing operation

`CommitAttemptRecord`:

- one concrete validate/publish attempt under a txn

`TxnLockRecord`:

- exclusive lifecycle-driving authority for one txn at a time

## 2.8.1 Job and Transaction Coupling

Allowed high-level coupling:

- a query job may have no txn
- an append/rewrite/maintenance/DDL finalization job may create one txn
- a txn-bearing job may not become `succeeded` before its txn becomes `committed`
- a txn-bearing job may not become terminal-success while its txn is `open`, `validating`, `committing`, or `unknown_outcome`

Expected mapping:

- `JobState=pending|planning|running|waiting_resource` -> txn absent or pre-finalization
- `JobState=committing` -> txn should exist
- `JobState=succeeded` -> txn absent or `TxnState=committed`
- `JobState=failed|canceled` -> txn absent, `aborted`, or unresolved only during recovery convergence

## 2.8.2 Transaction State Matrix

Phase 1 transaction states:

- `open`
- `validating`
- `committing`
- `committed`
- `aborting`
- `aborted`
- `unknown_outcome`

Allowed transitions:

- `open -> validating`
- `open -> aborting`
- `validating -> committing`
- `validating -> aborting`
- `committing -> committed`
- `committing -> unknown_outcome`
- `committing -> aborting` only if publish authority was not yet externally exercised
- `aborting -> aborted`
- `unknown_outcome -> committed`
- `unknown_outcome -> aborted`

Disallowed shortcuts:

- `open -> committed`
- `open -> unknown_outcome`
- `validating -> committed`
- `committed ->` any non-terminal state
- `aborted ->` any non-terminal state

Interpretation rule:

- `unknown_outcome` means BrewDB lost certainty about external publish truth, not that the txn is semantically failed

## 2.8.3 Commit Attempt State Matrix

Phase 1 commit-attempt states:

- `created`
- `validating`
- `publishing`
- `succeeded`
- `failed`
- `unknown_outcome`

Allowed transitions:

- `created -> validating`
- `validating -> publishing`
- `validating -> failed`
- `publishing -> succeeded`
- `publishing -> failed`
- `publishing -> unknown_outcome`

Disallowed shortcuts:

- `created -> publishing`
- `created -> succeeded`
- `failed -> publishing`
- `succeeded ->` any non-terminal state
- `unknown_outcome -> publishing`

Coupling rules:

- at most one active publish-bearing attempt may exist per txn at a time
- a new retry attempt requires ownership of `TxnLockRecord`
- `CommitAttemptRecord=succeeded` should imply `TxnState=committed` in the same convergence boundary
- `CommitAttemptRecord=unknown_outcome` should imply `TxnState=unknown_outcome`

## 2.8.4 Transaction Lock State Matrix

`TxnLockRecord` should be modeled as a lease-like authority object rather than a domain transaction state.

Useful lock states:

- `free`
- `held`
- `expired`

Allowed transitions:

- `free -> held`
- `held -> held` by heartbeat or fencing renewal
- `held -> expired`
- `expired -> held` by fenced reacquire
- `held -> free` by explicit release

Rules:

- only one `held` authority may be valid for the latest fencing epoch
- reconciliation reacquire must use a newer fencing epoch than the lost owner
- external publish callbacks or completions from stale epochs must be ignored

Implementation rule:

- `TxnLockRecord` may be persisted as a record with timestamps and epoch rather than an enum-backed state machine, but its semantics should still follow the transitions above

## 2.8.5 Cross-Object Invariants

The most important Phase 1 invariants are:

1. one job owner at a time may advance a non-recovery job lifecycle
2. one txn lock holder at a time may advance a txn lifecycle
3. one publish-bearing commit attempt at a time may represent the active external publish owner
4. `JobState=succeeded` implies no unresolved txn truth
5. `TxnState=unknown_outcome` blocks aggressive artifact cleanup
6. `TxnState=committed` allows success finalization only after commit-attempt convergence
7. `TxnState=aborted` allows failure or cancel finalization plus cleanup eligibility evaluation

## 2.8.6 Runtime State Ownership by Crate

Ownership should remain explicit:

- `brewdb-common` owns the shared enums and identity types
- `brewdb-runtime` owns the runtime records and allowed transition enforcement
- `brewdb-storage` may influence transaction outcome through validation/publish truth, but does not own the runtime transition graph
- `brewdb-execution` may complete tasks and emit artifacts, but does not advance txn or commit-attempt state directly

Framework rule:

- if a state transition changes job truth, txn truth, commit truth, or recovery truth, it belongs under `brewdb-runtime`

## 3. Dependency Direction

Recommended dependency direction:

- `brewdb-common`
- `brewdb-catalog -> brewdb-common`
- `brewdb-execution -> brewdb-common`
- `brewdb-storage -> brewdb-common + brewdb-catalog + selective brewdb-execution contracts`
- `brewdb-runtime -> brewdb-common + brewdb-catalog + brewdb-execution + brewdb-storage`
- `brewdb-sql -> brewdb-common + brewdb-catalog`
- `brewdb-planner -> brewdb-common + brewdb-catalog + brewdb-sql + brewdb-storage + brewdb-execution`

Key rules:

- `brewdb-common` depends on no other BrewDB crate
- `brewdb-sql` does not directly depend on storage semantics
- `brewdb-storage` does not depend on kernel orchestration
- `brewdb-execution` does not depend on kernel orchestration

## 4. Planner and Optimizer Placement

Planner responsibilities are split across three layers.

### `brewdb-sql`

Holds:

- SQL-facing planning
- binder logic
- statement classification
- frontend semantic rewrites
- lightweight frontend optimization

### `brewdb-planner`

Holds:

- planning entry for runtime-bound statements
- DataFusion logical optimization bridge
- distributed planning and distributed CBO
- fragment-local physical planning bridge
- planner diagnostics and distributed-plan assembly

The coordinator-side optimizer selection baseline for this layer is defined in `docs/coordinator-cbo-optimizer-selection.md`.

### Scheduler baseline

Phase 1 scheduler should be treated as:

- MPP-first in runtime behavior
- graph-wide in admission
- dependency-driven in dispatch
- boundary-aware in release conditions
- policy-extensible toward future BSP or superstep execution

Recommended ownership split:

- `brewdb-execution`
  - `StageGraph`
  - task dependency descriptors
  - boundary semantics such as `pipelined | materialized | barriered`
- `brewdb-runtime`
  - worker assignment
  - dispatch throttling
  - runnable-set release
  - retry policy
  - future scheduling policy selection

### `brewdb-execution`

Holds:

- physical execution planning
- stage shaping
- execution-facing optimization
- boundary placement logic

## 5. Module Layout by Crate

### `brewdb-common`

Recommended modules:

- `ids`
- `state`
- `catalog`
- `execution`
- `txn`
- `artifacts`
- `errors`
- `common`

`brewdb-common` should remain a stable shared language layer and avoid orchestration logic.

### `brewdb-catalog`

Recommended modules:

- `service`
- `path`
- `backend`
- `cache`
- `normalize`
- `model`

### `brewdb-sql`

Recommended modules:

- `parse`
- `ast`
- `bind`
- `analyze`
- `rewrite`
- `statement`
- `handoff`
- `capabilities`
- `errors`

### `brewdb-planner`

Recommended modules:

- `logical`
- `distributed`
- `physical`
- `diagnostics`
- `errors`

### `brewdb-execution`

Recommended modules:

- `plan`
- `task`
- `runtime`
- `boundaries`
- `artifacts`
- `worker`
- `cache`
- `metrics`

### `brewdb-storage`

Recommended modules:

- `engine`
- `scan`
- `append`
- `rewrite`
- `maintenance`
- `commit`
- `model`
- `table_engine`

### `brewdb-runtime`

Recommended modules:

- `jobs`
- `txns`
- `locks`
- `recovery`
- `leases`
- `mutation`
- `maintenance`
- `scheduler`
- `runtime_meta`

## 6. Binary Assembly

Crates are capability-oriented, and binary assembly should follow product or interface boundaries rather than internal runtime roles.

Phase 1 may expose binaries such as:

- `brewdbd`
- `brewdb`

## 7. Workspace Bootstrap

Phase 1 should start as one Rust workspace with capability crates and thin binaries.

Recommended top-level layout:

- `crates/brewdb-common`
- `crates/brewdb-catalog`
- `crates/brewdb-sql`
- `crates/brewdb-planner`
- `crates/brewdb-execution`
- `crates/brewdb-storage`
- `crates/brewdb-runtime`
- `bin/brewdbd`
- `bin/brewdb`

Non-crate top-level directories may include:

- `docs`
- `proto` or `api` for RPC contracts after Phase 1 interfaces stabilize
- `tests` for cross-crate integration scenarios

The workspace should begin with crate boundaries first, even if the first running binary keeps most paths in-process.

## 7.1 Repository Code Structure

Phase 1 should freeze a repository layout early so later code motion does not quietly redefine architecture.

Recommended top-level tree:

```text
BrewDB/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── clippy.toml
├── rustfmt.toml
├── docs/
├── crates/
│   ├── brewdb-common/
│   ├── brewdb-catalog/
│   ├── brewdb-sql/
│   ├── brewdb-planner/
│   ├── brewdb-execution/
│   ├── brewdb-storage/
│   └── brewdb-runtime/
├── bin/
│   ├── brewdbd/
│   └── brewdb/
├── tests/
│   ├── integration/
│   ├── fixtures/
│   └── harness/
└── scripts/
```

Top-level rules:

- all reusable product code lives under `crates/`
- all product-facing binaries live under `bin/`
- cross-crate integration tests live under `tests/`
- repo automation and local developer utilities live under `scripts/`
- architecture and design material stays under `docs/`

## 7.2 Standard Crate Skeleton

Every library crate should begin with a predictable internal shape.

Recommended baseline:

```text
crates/<crate-name>/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── errors.rs
│   ├── model/        # optional when a crate has a heavy model surface
│   ├── service/      # optional when a crate has orchestration/service entry points
│   └── ...
└── tests/            # only when crate-local integration tests are useful
```

Rules:

- `lib.rs` should be small and mostly define the public module map
- implementation-heavy helpers should stay below one extra directory level instead of crowding `lib.rs`
- crate-local `tests/` is for focused contract tests; cross-crate scenarios still belong in workspace `tests/`

## 7.3 Recommended Source Layout By Crate

The repo should not stop at crate names. Each crate should start with a predictable source tree.

### `crates/brewdb-common`

```text
src/
├── lib.rs
├── logging.rs
├── diagnostics.rs
├── errors.rs
└── config.rs
```

Rule:

- keep `brewdb-common` flat unless one module becomes too large; it is the shared foundation crate, not a deep service tree

### `crates/brewdb-catalog`

```text
src/
├── lib.rs
├── service.rs
├── model.rs
├── path.rs
├── backend.rs
├── errors.rs
├── store/
├── cache/
└── normalize/
```

Rule:

- stable caller-facing APIs stay near the top; transport and cache machinery move into subdirectories

Suggested catalog substructure responsibilities:

```text
service.rs     -> resolve catalog / database / table objects
path.rs        -> catalog.database.table naming and object-ref helpers
backend.rs     -> catalog backend and store-facing contracts
store/         -> FoundationDB catalog-store access boundary
cache/         -> normalized metadata cache boundary
normalize/     -> normalization boundary into BrewDB models
```

Catalog naming should stay unified:

- SQL-facing logical identity uses `catalog.database.table`
- `brewdb-catalog` should expose `TablePath` as the canonical table identity
- `brewdb-catalog` should model stable object identity separately through UUID-backed refs
- storage-binding details should stay inside table catalog entries rather than introducing a second public naming hierarchy

### `crates/brewdb-sql`

```text
src/
├── lib.rs
├── ast.rs
├── bind.rs
├── analyze.rs
├── rewrite.rs
├── intent/
│   ├── mod.rs
│   └── entry.rs
├── capabilities.rs
├── errors.rs
└── parse/
```

Rule:

- parser internals should stay behind `parse/`; upper layers should mostly see AST, bound forms, and intent outputs

### `crates/brewdb-planner`

```text
src/
├── lib.rs
├── logical/
├── distributed/
├── physical/
├── diagnostics.rs
└── errors.rs
```

Rule:

- planning glue to DataFusion should stay inside this crate; upper layers should mostly see optimized logical plans, `DistributedPlan`, and fragment-local planning artifacts

### `crates/brewdb-frontend`

```text
src/
├── lib.rs
├── errors.rs
├── pgwire/
├── session/
├── auth/
├── portal/
└── result/
```

Rule:

- external SQL protocol handling lives here; SQL semantics remain in `brewdb-sql`

### `crates/brewdb-execution`

```text
src/
├── lib.rs
├── plan.rs
├── protocol/
├── task.rs
├── boundaries.rs
├── artifacts.rs
├── errors.rs
├── stage_graph_builder/
├── runtime/
├── worker/
├── cache/
└── metrics/
```

Rule:

- contract modules stay at top level; executor/runtime implementation details go into subdirectories

Suggested worker substructure:

```text
worker/
├── mod.rs
├── task_executor.rs
├── stage_output_writer.rs
├── exchange_buffer_manager.rs
└── task_status_reporter.rs
```

Suggested protocol substructure:

```text
protocol/
├── mod.rs
├── coordinator_to_worker.rs
└── worker_to_coordinator.rs
```

### `crates/brewdb-storage`

```text
src/
├── lib.rs
├── engine.rs
├── model.rs
├── errors.rs
├── scan/
├── append/
├── rewrite/
├── maintenance/
├── commit/
├── statistics/
├── paimon/
└── iceberg/
```

Rule:

- format-neutral contracts stay at top level; concrete table-engine implementations stay in per-format modules

### `crates/brewdb-runtime`

```text
src/
├── lib.rs
├── errors.rs
├── admission/
├── finalization/
├── jobs/
├── txns/
├── locks/
├── commit/
├── recovery/
├── leases/
├── mutation/
├── maintenance/
├── planning/
└── runtime/
```

Rule:

- runtime state and orchestration subsystems should live in dedicated directories, because this crate will grow fastest

## 7.4 Binary Package Layout

Binary packages should stay thin and mostly assemble shared crates.

### `bin/brewdbd`

Recommended structure:

```text
bin/brewdbd/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── config.rs
    ├── bootstrap.rs
    ├── server.rs
    └── wiring/
```

### `bin/brewdb`

Recommended structure:

```text
bin/brewdb/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── cli.rs
    ├── config.rs
    └── commands/
```

Binary rules:

- binaries should wire config, dependency assembly, and interface surfaces
- binaries should not become the home of shared domain logic
- if logic becomes reusable across binaries, move it back to `crates/`

## 7.5 Test Layout

Testing should also follow a fixed repository shape.

Recommended layout:

```text
tests/
├── integration/
│   ├── append_flow.rs
│   ├── commit_recovery.rs
│   └── mutation_flow.rs
├── fixtures/
│   ├── catalog/
│   ├── runtime/
│   └── storage/
└── harness/
    ├── mod.rs
    ├── cluster.rs
    ├── catalog.rs
    └── object_store.rs
```

Rules:

- reusable fake services and test harness code live under `tests/harness`
- static inputs and golden data live under `tests/fixtures`
- scenario-oriented end-to-end tests live under `tests/integration`

## 7.6 Code Structure Freeze Rules

Once implementation starts, the following structure rules should be treated as frozen unless an architecture review reopens them:

1. shared logic does not move into `bin/`
2. format-specific code does not leak out of `brewdb-storage/formats/`
3. runtime truth and orchestration code does not leak out of `brewdb-runtime/`
4. cross-crate integration scenarios do not get buried inside one library crate
5. top-level repository directories are added only for a new architecture concern, not short-term convenience

## 7.7 Workspace Cargo Strategy

The workspace should also freeze how Cargo metadata is organized, so dependency sprawl does not redefine architecture by accident.

Recommended workspace root files:

- `Cargo.toml`
- `Cargo.lock`
- `rust-toolchain.toml`
- `clippy.toml`
- `rustfmt.toml`

Recommended root `Cargo.toml` responsibilities:

- declare all workspace members
- define shared edition, version, license, and repository metadata
- define `[workspace.dependencies]` for deliberately shared dependencies
- define shared lint policy once the codebase begins to compile

The root manifest should not:

- become the home of product code
- hide crate boundaries with path tricks
- turn every third-party library into a mandatory global dependency

## 7.8 Workspace Dependency Policy

Dependencies should be shared only when that reduces real inconsistency.

### Put in `[workspace.dependencies]`

Good candidates:

- foundational libraries used across many crates
- async/runtime primitives if Phase 1 standardizes on one stack
- serialization crates used in shared contracts
- tracing/logging crates
- error-handling crates
- testing crates used consistently across workspace tests

Examples of likely shared categories:

- `anyhow` or equivalent application-error layer
- `thiserror`
- `serde`
- `tracing`
- `tokio` if runtime standardization is explicit

### Keep local to a crate

Good candidates:

- parser-specific libraries used only by `brewdb-sql`
- format-specific libraries used only by `brewdb-storage`
- transport-specific client libraries used only by binaries or catalog internals
- experimental dependencies not yet proven to be cross-workspace standards

Rule:

- a dependency belongs in `[workspace.dependencies]` only after it is clearly part of the shared engineering baseline, not merely because two crates happen to use it today

## 7.9 Dependency Direction Enforcement

The workspace should encode architectural direction in Cargo layout as much as possible.

Rules:

- `brewdb-common` must not depend on any BrewDB crate
- `brewdb-sql` must not depend directly on `brewdb-storage`
- `brewdb-execution` must not depend on `brewdb-runtime`
- `brewdb-storage` must not depend on `brewdb-runtime`
- `bin/*` may depend on library crates, but library crates must not depend on `bin/*`

Practical policy:

- when a dependency direction feels wrong, prefer moving a shared type downward into `brewdb-common` or narrowing a contract module rather than adding a reverse dependency

## 7.10 Feature Flag Strategy

Phase 1 should keep feature flags conservative.

Allowed feature-flag uses:

- optional format implementations inside `brewdb-storage`
- optional binaries or admin surfaces
- optional integration-test scaffolding

Avoid:

- feature flags that radically change core ownership boundaries
- circular "optional" dependency patterns that effectively bypass architecture rules
- transport/runtime flags that create multiple incompatible shared contract surfaces too early

Recommended shape:

- `brewdb-storage` may use format flags such as `paimon` and `iceberg`
- workspace integration tests may gate external-system fixtures separately
- core state and runtime truth types should not be feature-fragmented in Phase 1

## 7.11 Lint and Formatting Policy

Engineering consistency should be part of the code structure freeze.

Recommended policy:

- one workspace formatter configuration
- one workspace lint baseline
- crate-local lint suppressions only with a concrete reason

Rules:

- formatting should be repo-wide and non-controversial
- lint suppression should stay near the code it justifies
- architectural layering problems should not be papered over with lint ignores

## 7.12 Build and Test Command Baseline

The repo should standardize a small set of baseline commands from the beginning.

Recommended baseline:

- `cargo check --workspace`
- `cargo test --workspace`
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets`

Later additions may include:

- focused integration-test invocations
- format-specific test jobs
- local cluster smoke tests

Rule:

- if a new crate or binary meaningfully changes the baseline developer command set, that should be called out in architecture review rather than introduced silently

## 7.13 Module Visibility and Naming Conventions

The workspace should also freeze a few code-shape rules inside `src/` trees, so module growth stays legible.

### Visibility defaults

Recommended defaults:

- modules are private unless they are part of the crate's architecture surface
- helpers default to `pub(crate)` instead of `pub`
- structs and enums default to private fields unless external construction is part of the contract
- constructors and builders should be explicit rather than exposing writable state everywhere

Rules:

- if a symbol is only used inside one crate, do not make it public for convenience
- if a symbol is used by tests only, prefer test-local helpers or crate-local exposure instead of widening the public API
- widening visibility should be treated as an architecture change, not a casual refactor

### `lib.rs` policy

Each library crate should use `lib.rs` as a boundary map, not an implementation file.

Recommended shape:

- declare the crate's stable modules
- re-export a small number of intentionally stable entry points when useful
- avoid hiding deep implementation structure behind giant re-export blocks

Avoid:

- putting substantive orchestration or parsing logic directly in `lib.rs`
- turning `lib.rs` into a convenience dump of every submodule
- using wildcard public exports at the crate root

### Naming rules

Recommended naming bias:

- nouns for domain types such as `JobRecord`, `TableCatalogEntry`, `TaskResult`
- verbs or verb phrases for operations such as `validate_commit`, `resolve_table`, `build_distributed_plan`
- plural module names for grouped domains such as `jobs`, `txns`, `leases`
- singular module names for narrow contract surfaces such as `engine`, `path`, `model`

Avoid:

- vague module names such as `utils`, `misc`, `helpers`, `common2`
- role-ambiguous names that blur crate ownership
- abbreviations that are not already standard in the codebase

## 7.14 Internal Layering Inside Crates

Even within a crate, the code should still respect a small number of internal layers.

Recommended internal layering shape:

1. model or contract layer
2. service or orchestration layer
3. persistence, transport, or integration layer
4. local helper implementations

Examples:

- `brewdb-runtime`: records and state types -> orchestration services -> runtime-meta backend integrations
- `brewdb-catalog`: normalized models -> `CatalogService` -> cache/store integrations
- `brewdb-storage`: engine contracts -> operation realizations -> concrete `TableEngine` implementations

Rule:

- lower layers should not reach upward into orchestration logic just because they live in the same crate

## 7.15 Error Boundary Policy

Error shape is part of code structure because it defines how crates talk to each other under failure.

Recommended policy:

- each crate owns a small crate-local error surface
- cross-crate contracts should prefer structured errors over ad hoc strings
- `brewdb-common` may define shared error categories when they are genuinely common domain concepts
- stable error codes should live in `brewdb-common::diagnostics` even when error enums stay crate-local

Rules:

- `brewdb-catalog` should not leak raw FoundationDB client errors or record-layout details as its stable public error type
- `brewdb-storage` should not leak format-vendor error types as its stable public contract
- `brewdb-runtime` should translate storage/catalog/execution failures into runtime-relevant orchestration errors
- binaries may further wrap errors for CLI/server reporting, but should not become the canonical home of shared error semantics
- logging should emit structured events with stable `target`, `event_name`, `error_code`, and `job_id`
- the process-global tracing subscriber should also be the collection point for upstream engine logs such as DataFusion targets

Dependency direction for the first external integrations:

- `brewdb-catalog` may depend on FoundationDB client libraries for catalog persistence, but must still normalize stored records before exposing them upward
- `brewdb-storage` may depend directly on `paimon` for table-engine-native table/catalog access
- `brewdb-catalog` is not architecturally coupled to an external catalog service
- within `brewdb-catalog::store`, the primary direction is FoundationDB-backed persistence rather than remote REST mediation

## 7.16 Serialization Boundary Policy

Serialization concerns should be explicit so stable domain types do not accidentally become transport-first types.

Recommended policy:

- derive serialization on shared types only when there is a real cross-process or persistence need
- keep transport DTOs close to the transport boundary when they diverge from internal domain types
- do not let external API shape silently dictate internal type ownership

Rules:

- `brewdb-common` may carry serialization derives for true shared records and ids
- execution wire payload specifics should stay near `brewdb-execution` contracts or future transport modules
- binary-layer API payload wrappers should not leak backward into crate ownership decisions

## 8. Initial Interface Boundaries

Before detailed Rust traits are frozen, Phase 1 should preserve these interface seams.

### `brewdb-common`

Must define stable shared types for:

- ids for job, stage, task, txn, artifact, catalog, database, and table objects
- lifecycle enums
- error families
- basic capability descriptors
- lightweight request context and tenancy context

### `brewdb-catalog`

Should expose a `CatalogService`-oriented API for:

- catalog/database/table resolution
- `TableCatalogEntry` loading
- table storage binding lookup
- stable table-format routing inputs

It should hide catalog-store-specific backend details from upper layers.

### `brewdb-sql`

Should emit statement and planner-handoff outputs rather than direct execution plans.

Useful output families:

- parsed statement
- bound statement
- statement family
- planner handoff for runtime-bound statements

### `brewdb-execution`

Should expose execution contracts, not coordinator ownership logic.

Useful contracts include:

- stage graph
- task request / task result
- stage boundary descriptors
- artifact result descriptors
- Arrow-native batch / stream result contracts for execution-time data movement

### `brewdb-storage`

Should expose engine-facing capabilities for:

- scan planning inputs
- append/rewrite materialization contracts
- validation and commit preparation
- maintenance strategy selection

### `brewdb-runtime`

Should own the main orchestration interfaces:

- job submission
- planning handoff
- dispatch coordination
- txn lifecycle handling
- commit orchestration
- recovery entry points

## 9. System Bring-Up Order

After the framework shell exists, implementation should advance by closed system loops rather than by crate.

The main system lines are:

- request entry
- control plane
- planning
- execution
- storage/format
- txn/lifecycle

Horizontal infrastructure must follow every loop, not be deferred to the end:

- logging
- error translation
- config
- metrics and observability
- request correlation
- test harness
- external dependency strategy

Recommended loop order:

1. minimal query closed loop
2. distributed query dispatch loop
3. result return and coordinator aggregation loop
4. mutation finalization and txn loop
5. storage-format deepening loop
6. recovery and reconciliation loop

Rationale:

- loop 1 proves that BrewDB can accept a request and drive it through the full coordinator-worker skeleton
- loop 2 makes the system truly MPP-shaped instead of local-runtime-shaped
- loop 3 closes user-visible query semantics before mutation complexity is added
- loop 4 introduces commit-bearing lifecycle logic after the non-commit query path is stable
- loop 5 deepens format-aware planning only after upper-layer orchestration is real
- loop 6 hardens the system once normal-path ownership boundaries are already proven

Crate bring-up still matters, but it is now subordinate to the system loop order above.

## 10. Phase 1 Walking Skeleton

The first end-to-end development milestone should be a minimal query closed loop, not an append-first path.

### Goal

Prove that one SQL query can enter BrewDB, become a distributed execution graph, run through the coordinator-worker contract, and return a result shell to the caller.

### In scope

1. `brewdb` or `brewdbd` accepts one query request
2. `brewdb-sql` parses, binds, and classifies the statement
3. `brewdb-catalog` resolves `TableCatalogEntry` objects needed for planning and execution
4. `brewdb-planner` builds the optimized logical plan and `DistributedPlan`
5. `brewdb-runtime` admits the request and allocates runtime identity
6. `brewdb-storage` provides scan-facing planning inputs and statistics shells
7. `brewdb-execution` turns each fragment-local logical plan into a DataFusion physical execution tree
8. the scheduler admits the full graph at once and dispatches runnable tasks by dependency readiness
9. workers execute plan slices and cross exchange boundaries
10. worker task results return through the execution protocol
11. the coordinator aggregates final query outputs and returns a result stream or result-batch shell

### Out of scope

- final table commit
- mutation artifact publish
- recovery after coordinator loss
- durable shuffle as a recovery contract
- long-running resumability
- full cost-based optimization

### Ownership boundary by step

1. request parsing and statement entry: `brewdb`, `brewdbd`, `brewdb-sql`
2. request admission, job identity, correlation context: `brewdb-runtime`
3. catalog/database/table resolution: `brewdb-catalog`
4. format-aware scan requirements: `brewdb-storage`
5. distributed planning and fragment shaping: `brewdb-planner`
6. fragment-local physical planning and task contracts: `brewdb-execution`
7. graph admission, dependency-driven dispatch, worker assignment: `brewdb-runtime`
8. operator execution and exchange behavior: `brewdb-execution`
9. result shaping and return path: `brewdb-execution` plus `brewdb-runtime`

### Why query first

- it validates the main MPP control loop without dragging commit truth into the first milestone
- it keeps BrewDB aligned with DataFusion's strongest native path first
- it forces coordinator, scheduler, worker, and protocol boundaries to become real before mutation shortcuts appear
- it creates the cleanest baseline for later append, rewrite, and maintenance job families

### Validation milestone

Phase 1 query skeleton is complete when one query can:

- enter through the server or CLI boundary
- produce a runtime job context
- build a full `DistributedPlan` and its projected stage view
- dispatch tasks to at least one worker path
- return a final query result shell with correlated diagnostics

## 11. Testing Strategy by Layer

Phase 1 should test by closed loop and by boundary, not only by crate.

Recommended test emphasis:

- `brewdb-common`: pure unit tests for logging config, diagnostics primitives, and foundational invariants
- `brewdb-catalog`: `CatalogService` tests with mocked catalog-store reads and writes
- `brewdb-storage`: `TableEngine` contract tests per format
- `brewdb-execution`: task contract, boundary, and artifact result tests
- `brewdb-runtime`: job lifecycle, txn state, commit retry, and recovery tests
- workspace integration tests: query skeleton, dispatch-path failure, and result aggregation correctness

The first integration tests should focus on the minimal query closed loop, because it is the first system truth that all later mutation and recovery work will build on.

## 12. Early Non-Goals for Code Structure

Phase 1 should avoid:

- separate crates for every submodule too early
- format-specific orchestration logic leaking into `brewdb-runtime`
- direct SQL-layer dependence on concrete catalog-store or format clients
- worker binaries owning commit or catalog mutation logic
- premature RPC schema freeze before task/result contracts stabilize

The codebase should first optimize for clear ownership boundaries and a working lifecycle skeleton, then for service decomposition detail.

Those binaries should be assembly layers over shared crates. They should not define the crate structure.

## 13. Development Architecture Baseline

The following development decisions should be treated as the default Phase 1 baseline unless a later architecture review explicitly changes them.

### Repository shape

- one Rust workspace
- capability-oriented crates
- thin product-oriented binaries
- one repository for runtime, tooling, shared kernels, and integration tests

### Runtime topology for implementation

- coordinator and worker remain runtime responsibilities, not package boundaries
- Phase 1 may host those responsibilities inside one server binary
- distributed boundaries must still exist in code even when local execution is used
- the first production-shaped milestone is query-first and MPP-first in scheduling behavior

### Metadata split

- BrewDB-owned catalog metadata remains outside BrewDB runtime ownership
- BrewDB runtime metadata is a separate logical store
- both logical stores use FoundationDB in Phase 1, but remain separate in role and keyspace
- format-native metadata remains table-engine-owned truth
- object storage remains artifact and data truth

### Planning split

- SQL parsing, binding, and statement routing in `brewdb-sql`
- distributed planning and distributed CBO in `brewdb-planner`
- orchestration planning in `brewdb-runtime`
- fragment-local physical planning in `brewdb-execution`

### Commit split

- workers may produce artifact-bearing outputs
- only the coordinator-side kernel may advance commit state
- only storage table engines may interpret and publish format truth

### Table-engine split

- one `TableEngine` per resolved table format
- table engines may hide sub-components internally
- upper layers must not bind directly to format-specific metadata models

## 14. Architecture Freeze Checklist

Before implementation starts, the development architecture should be considered frozen only if the following questions are answered with an explicit "yes" in review.

1. Are crate boundaries final enough to prevent runtime-role code sprawl?
2. Is the ownership split between `brewdb-runtime`, `brewdb-execution`, and `brewdb-storage` clear enough that commit, task execution, and format semantics will not mix?
3. Is `CatalogService` the only catalog entry used by planning and commit flows?
4. Is the runtime metadata store explicitly separate in role from the built-in catalog even though both use FoundationDB?
5. Is the first walking skeleton agreed to be query-first rather than append-first?
6. Are worker outputs defined as non-authoritative staged artifacts rather than direct table-visible commits?
7. Are mutation and maintenance paths staying inside the same lifecycle framework instead of side tooling?

If any answer is "no", the architecture is still fluid and implementation should not begin.

## 15. Open Decisions To Lock Before Coding

The architecture is already strong on logical boundaries, but a few development-shaping decisions still need explicit confirmation.

### A. Workspace package layout

Recommended default:

- `crates/*` for shared libraries
- `bin/*` for binaries

Alternative:

- all packages under `crates/*`, including binaries

The main question is consistency, not capability. I recommend `crates/*` plus `bin/*` because it keeps product assembly visually separate from reusable kernels.

Decision recommendation:

- adopt `crates/*` plus `bin/*` as the Phase 1 repository convention

Why this should be the default:

- makes reusable kernels visually primary
- reduces the chance that server or tooling assembly code starts attracting shared logic
- keeps the workspace readable once integration tests and support packages appear

What this decision rules out:

- runtime-role packaging becoming the main mental model of the repository
- binaries quietly turning into ownership centers for domain logic

### B. RPC contract timing

Recommended default:

- do not freeze RPC schemas yet
- keep request/result contracts stable inside Rust modules first
- move wire-schema extraction later

This matches the existing Phase 1 bias against premature interface freeze.

Decision recommendation:

- do not freeze RPC or service IDL in early Phase 1

Why this should be the default:

- task/result contracts are still architecture-shaping, not just transport-shaping
- execution and commit boundaries need to settle before a durable wire schema is worth defending
- early IDL freeze would push accidental coupling into runtime internals

What this decision rules out:

- designing transport messages before execution contracts stabilize
- turning temporary local shapes into long-lived compatibility obligations

### C. Local execution mode

Recommended default:

- keep a local single-process development mode
- preserve runtime responsibility boundaries even in local mode

This reduces bring-up cost without collapsing the distributed design.

Decision recommendation:

- keep a first-class local single-process mode for development and integration testing

Why this should be the default:

- shortens bring-up and debug loops
- allows the walking skeleton to validate orchestration without waiting for full distributed deployment tooling
- preserves architecture truth as long as dispatch, task contract, and commit boundaries remain explicit in code

What this decision rules out:

- an in-process shortcut that bypasses task contracts
- folding execution runtime logic directly into orchestration modules

### D. Initial table-engine target

Recommended default:

- make Paimon the first full table-engine target
- keep Iceberg as a planned second table-engine surface

That aligns with the current BrewDB-owned catalog plus Paimon-first table-engine direction and avoids pretending both formats will mature at the same speed on day one.

Decision recommendation:

- treat Paimon as the first complete table-engine target and Iceberg as interface-following Phase 1 scope

Why this should be the default:

- current catalog direction already favors BrewDB-owned catalog routing with Paimon as the first deep table-engine target
- one serious table-engine target is better for pressure-testing mutation, maintenance, and reconciliation boundaries than two partial targets
- Iceberg can still shape generic table-engine contracts without forcing equal implementation depth

What this decision rules out:

- false symmetry between Paimon and Iceberg in the first implementation wave
- delaying architecture validation until two table engines advance in parallel

## 16. Freeze Draft

If the project wants a concrete development-architecture freeze position today, the recommended Phase 1 answers are:

1. Repository layout: `crates/*` for shared libraries, `bin/*` for product-facing binaries such as `brewdbd` and `brewdb`
2. RPC timing: internal Rust contracts first, no early wire-schema freeze
3. Local mode: supported and encouraged, but must preserve runtime responsibility boundaries
4. Initial format target: Paimon first, Iceberg second

Together these choices optimize for boundary correctness over premature service decomposition.

They also fit the existing architecture constraints:

- capability-oriented crates stay primary
- transport remains secondary to execution and commit semantics
- distributed behavior is preserved even during local bring-up
- table-engine abstractions are validated against one deep target before being generalized too aggressively

## 17. Approval Matrix

Use this matrix when you want to explicitly freeze the development architecture in review.

### Decision 1: repository layout

- recommended: approve `crates/*` + `bin/*`
- reject if you want binaries to live under `crates/*`
- effect of rejection: low architectural risk, mostly repository convention churn

### Decision 2: RPC timing

- recommended: approve late wire-schema freeze
- reject if you want IDL-first development
- effect of rejection: high coupling risk across runtime and execution contracts

### Decision 3: local execution mode

- recommended: approve first-class local mode with preserved boundaries
- reject if you want distributed-only bring-up
- effect of rejection: slower iteration and higher infrastructure cost early

### Decision 4: initial table-engine target

- recommended: approve Paimon-first
- reject if you want equal Paimon/Iceberg depth from the start
- effect of rejection: slower table-engine boundary validation and wider early scope

## 18. Implementation Gate

Implementation should begin only after the four approval-matrix decisions above are either:

- explicitly approved as written, or
- replaced with alternate choices recorded in this document

Until then, this document should be treated as architecture work product, not implementation guidance only.

## 19. Design Rules

1. Crates are organized by stable kernel capability, not by deployment role.
2. Shared domain objects belong in `brewdb-common`; orchestration does not.
3. SQL frontend, lifecycle orchestration, execution, and storage semantics remain separate layers.
4. `brewdb-storage` owns storage semantics, while `brewdb-execution` owns execution runtime behavior.
5. Binaries assemble capabilities; they do not define capability boundaries.
