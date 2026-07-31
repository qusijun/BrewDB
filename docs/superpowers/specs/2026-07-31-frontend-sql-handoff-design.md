# Frontend to SQL Handoff Design

Date: 2026-07-31

## Goal

Define a stable, protocol-neutral handoff from `brewdb-frontend` into `brewdb-sql` so that:

- frontend owns client/session/request truth
- sql owns statement-classification truth
- protocol adapters such as pgwire and future ADBC ingress can share the same SQL entry boundary

This design intentionally stops at the frontend-to-sql boundary. It does not introduce runtime admission, planner execution, or protocol-specific result transport changes.

## Problem Statement

`brewdb-frontend` already has protocol-neutral session/request shells and a minimal pgwire adapter shell. `brewdb-sql` is still only a skeleton crate. Without a formal handoff between them, frontend code would either:

- depend directly on SQL implementation details too early, or
- pass ad hoc SQL strings and scattered session fields into later layers

That would make the boundary unstable and would make a future protocol expansion, especially ADBC, more expensive than it needs to be.

## Architecture Decision

Introduce a formal SQL ingress contract owned by `brewdb-sql`.

The handoff flow becomes:

`PgWireRequest -> ClientSqlRequest -> SqlIngressRequest -> SqlStatementEnvelope`

Responsibilities remain split as follows:

- `brewdb-frontend`
  - protocol adapters
  - session and request lifecycle
  - frontend statement routing
  - translation from `ClientSqlRequest` into SQL ingress input
- `brewdb-sql`
  - ingress validation
  - statement classification truth
  - stable outward statement envelope for planner-facing growth

This keeps `brewdb-sql` as the owner of SQL-layer interpretation while preventing protocol-specific fields from leaking below frontend.

## Considered Approaches

### Recommended: SQL-owned ingress contract

Create a frontend-facing SQL entry object and a SQL-owned statement envelope.

Why this is preferred:

- frontend depends on one stable SQL surface
- sql can evolve internally from simple classification into parsed/bound/plannable forms
- protocol neutrality stays enforceable at the handoff line

Trade-off:

- requires a few new contract types before real parser work exists

### Thinner direct analysis API

Expose a simple `analyze(sql_text)` API and let frontend pass extra context as needed.

Why this was rejected:

- encourages scattered context fields
- makes it harder to enforce protocol-neutral shape
- likely to create a soft dependency on SQL internals

### Heavier full parser-plus-binder shell

Define `ParsedStatement`, `BoundStatement`, and planner-handoff shells immediately.

Why this was rejected for now:

- `brewdb-sql` is still mostly empty
- adds abstraction weight before the first stable ingress boundary is proven
- risks inventing placeholder semantics just to look complete

## Contract Design

### Frontend-owned input truth

`brewdb-frontend` continues to own:

- `ClientSqlRequest`
- `ClientContext`
- `RequestContext`
- frontend routing decision (`session-local` vs `runtime-bound`)

Frontend does not hand raw protocol messages to SQL.

### SQL-owned ingress types

`brewdb-sql` should introduce:

- `SqlIngressRequest`
- `SqlSessionContext`
- `SqlRequestContext`
- `FrontendStatementRoute`
- `SqlDriver`

Recommended minimal field shape for `SqlIngressRequest`:

- `session_id`
- `request_id`
- `user_name`
- `database_name`
- `catalog_name` as optional
- `sql`
- `route_scope`
- `client_capabilities` as optional protocol-neutral capability metadata

### SQL-owned outward result

`brewdb-sql` should return a single outward result family through:

- `SqlStatementEnvelope`
- `StatementCategory`
- `SessionStatement`
- `RuntimeStatement`

At this stage the envelope remains intentionally thin. It should at least carry:

- original statement text
- normalized statement name
- statement category
- route scope

`RuntimeStatement` is a forward-compatible shell. It does not yet need to contain a real `ParsedStatement` or `BoundStatement`, but it should leave room for those additions without forcing frontend changes later.

## Protocol Neutrality Rules

The SQL handoff must remain compatible with future non-pgwire clients such as ADBC.

### Allowed in `SqlIngressRequest`

- session identity
- request correlation
- default catalog/database context
- SQL text
- frontend route scope
- protocol-neutral client capabilities

### Forbidden in `SqlIngressRequest`

- pgwire frame/message details
- PostgreSQL-specific protocol parameters or status fields
- raw socket/listener transport metadata
- ADBC-specific handles or stream objects
- a discriminator that says which protocol adapter produced the request

This keeps protocol branching above the SQL handoff instead of inside it.

## ADBC Extension Path

If BrewDB later adds ADBC ingress, the primary implementation surface should be in `brewdb-frontend`, not `brewdb-sql`.

Expected extension points:

- add `crates/brewdb-frontend/src/adbc/`
- possibly extend `ClientCapabilities`
- possibly extend frontend result shaping for Arrow-oriented result delivery

Expected stable areas:

- session/request contracts
- `ClientSqlRequest`
- `SqlIngressRequest`
- `SqlStatementEnvelope`
- `brewdb-sql` statement classification boundary

This means pgwire and ADBC should diverge before `ClientSqlRequest` and converge again at `SqlIngressRequest`.

## Error Model

The first version should keep error shape narrow:

- invalid request
  - empty SQL
  - missing required context
- route conflict
  - frontend route scope and SQL-determined category are incompatible
- unsupported statement
  - statement is recognized enough to classify but not yet supported for deeper analysis

This is enough to make the boundary observable without prematurely designing a large SQL diagnostics hierarchy.

## File Layout

### `crates/brewdb-frontend`

Expected changes:

- extend `src/session/mod.rs`
  - frontend-to-sql handoff adapter logic
- possibly add a small dedicated adapter module later if the mapping grows

Frontend should remain the owner of translation from client request truth into SQL ingress truth.

### `crates/brewdb-sql`

Expected first files:

- `src/lib.rs`
  - crate exports
- `src/ingress.rs`
  - ingress request and service boundary
- `src/statement.rs`
  - outward statement envelope and category shells
- `src/errors.rs`
  - SQL entry errors

This keeps the SQL entry boundary explicit and isolated from future parser/binder growth.

## Data Flow

The minimal closed boundary for this milestone is:

1. protocol adapter creates or resumes frontend session state
2. frontend builds `ClientSqlRequest`
3. frontend maps request into `SqlIngressRequest`
4. sql validates request and classifies statement
5. sql returns `SqlStatementEnvelope`
6. frontend decides whether the next step stays local or moves toward planner/runtime later

For this milestone, step 6 ends at the envelope. No runtime or planner execution is added yet.

## Testing Strategy

### `brewdb-sql`

Add unit tests that prove:

- a session-local ingress request returns a session statement envelope
- a runtime-bound ingress request returns a runtime statement envelope
- empty SQL returns an invalid-request error
- route conflicts return a route-conflict error

### `brewdb-frontend`

Add unit tests that prove:

- `ClientSqlRequest` maps to `SqlIngressRequest` with the expected identity and request fields
- protocol-specific details do not appear in the SQL handoff contract

The tests should verify the boundary shape rather than parser completeness.

## Non-Goals

This design does not include:

- real SQL parsing or binding
- full planner handoff semantics
- runtime admission
- ADBC protocol implementation
- full prepared-statement lifecycle
- PostgreSQL protocol completeness

## Acceptance Criteria

This milestone is complete when:

- `brewdb-sql` exposes a formal frontend-facing ingress contract
- `brewdb-frontend` depends only on that SQL contract rather than SQL internals
- the ingress contract is protocol-neutral and contains no pgwire-specific fields
- the first statement envelope shape is test-covered
- future ADBC ingress can be added primarily as a new frontend adapter instead of a SQL-boundary redesign
