# Codex Constraints

This file records repository-specific constraints for Codex and other coding
agents working on BrewDB.

## Workflow

- Read the existing code before changing behavior. Prefer local patterns over
  introducing new abstractions.
- Use `rg` for code search.
- Use `apply_patch` for manual edits.
- Do not revert or overwrite user changes unless explicitly asked.
- Keep changes scoped to the requested task.
- After Rust code changes, run `cargo fmt`, `cargo check -p brewdb`, and focused
  tests for the touched area. Run broader tests when changing shared runtime,
  planner, storage, or catalog behavior.

## Rust Structure

- Keep `mod.rs` files light. They should mostly declare modules and re-export
  public APIs.
- Put core logic in focused files such as `engine.rs`, `errors.rs`, or
  domain-specific modules.
- Avoid request/response-style wrapper types for local planner/runtime
  boundaries unless they model a real RPC boundary.
- Prefer strongly typed domain enums over stringly typed control-plane state.

## Errors

- Module-local errors should integrate with the BrewDB diagnostics framework by
  implementing `DiagnosticError`.
- Use stable `BREWDB_<MODULE>_...` error codes.
- Keep broad string errors at external boundaries only; internal routing should
  use typed variants.

## Storage

- `StorageEngine` is a process-level registry/facade that derives
  `TableEngine`s from `TableCatalogEntry`.
- Concrete table engines are registered through `TableEngineFactory` plugins.
- Storage registry keys should use `StorageKind`, not raw strings.
- Memory tables should behave like ordinary table engines and should not add
  DataFusion `MemTable` knowledge to the storage registry.

## Planner And Runtime

- Keep logical planning, fragment planning, scheduling, exchange, and local
  execution as separate layers.
- `FragmentPlanner` is the planner-facing abstraction. Standalone and
  distributed modes should consume the same storage registry shape.
- Execution-side exchange services belong to runtime/execution boundaries, not
  logical fragment definitions.

## Documentation

- Keep docs short and current.
- Remove obsolete design documents instead of preserving stale architecture.
- Put user-facing startup instructions in `docs/QuickStart.md` and keep them in
  sync with `README.md`.
