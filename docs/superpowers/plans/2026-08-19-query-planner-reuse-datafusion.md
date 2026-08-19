# Query Planner Reuse DataFusion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make BrewDB's `SELECT` statement planning follow DataFusion's statement-to-logical-plan shape more closely so aggregate rewrites stay inside the logical planner instead of leaking into physical planning.

**Architecture:** Keep BrewDB's own statement shell and catalog/session resolution, but make the query branch build plans the same way DataFusion does: bind expressions, detect aggregates recursively, build aggregate nodes directly, and avoid projecting aggregate expressions back into the plan. BrewDB-specific table binding and distributed rewrite layers stay in place.

**Tech Stack:** Rust, DataFusion logical plan builder, `datafusion_common::tree_node`, BrewDB planner tests.

---

### Task 1: Normalize aggregate detection in query planning

**Files:**
- Modify: `crates/brewdb/src/planner/logical/query.rs:80-182`

- [ ] **Step 1: Keep aggregate detection recursive**

Use DataFusion's tree traversal to detect nested aggregate expressions, including aliases:

```rust
fn expr_contains_aggregate(expr: &DataFusionExpr) -> bool {
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};

    let mut found = false;
    let _ = expr.apply(|node| {
        if matches!(node, DataFusionExpr::AggregateFunction(_)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}
```

- [ ] **Step 2: Build aggregate plans directly**

When `needs_aggregate(query)` is true, collect aggregate-bearing projection items, build the `Aggregate` node, apply `HAVING`, and return the aggregate plan immediately instead of projecting the original expressions back on top.

```rust
if needs_aggregate(query) {
    let aggregates = query
        .projection
        .iter()
        .filter(|expr| expr_contains_aggregate(expr))
        .cloned()
        .collect::<Vec<_>>();
    input = LogicalPlanBuilder::from(input)
        .aggregate(group_keys, aggregates)
        .map_err(map_df_plan_error)?
        .build()
        .map_err(map_df_plan_error)?;
    if let Some(predicate) = &query.having {
        input = LogicalPlanBuilder::from(input)
            .filter(predicate.clone())
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    return Ok(input);
}
```

- [ ] **Step 3: Keep the non-aggregate path unchanged**

Leave the passthrough wildcard and normal projection path intact for non-aggregate queries.

### Task 2: Lock the behavior with planner tests

**Files:**
- Modify: `crates/brewdb/src/planner/mod.rs:403-421`

- [ ] **Step 1: Keep the aliased aggregate regression test**

Assert that `select count(id) as lineitem_count from orders` produces an `Aggregate` root, not a projection with raw aggregate expressions.

```rust
#[test]
fn logical_planner_keeps_aliased_aggregate_out_of_projection() {
    let plan = build_query_plan(
        "select count(id) as lineitem_count from orders",
        vec![make_table("orders")],
    );
    let DataFusionLogicalPlan::Aggregate(aggregate) = plan.fragments[0]
        .root
        .as_ref()
        .expect("expected aggregate root")
    else {
        panic!("expected aggregate root");
    };
    assert_eq!(aggregate.aggr_expr.len(), 1);
}
```

- [ ] **Step 2: Keep the existing aggregate-tree test passing**

Verify `select count(id) from orders` still produces the distributed aggregate fragment layout used by the runtime tests.

### Task 3: Verify the query path end to end

**Files:**
- None

- [ ] **Step 1: Run focused planner tests**

Run:

```bash
cargo test -p brewdb logical_planner_keeps_aliased_aggregate_out_of_projection -- --nocapture
cargo test -p brewdb distributed_planner_builds_aggregate_tree -- --nocapture
```

Expected: both pass.

- [ ] **Step 2: Run the aggregate runtime regression**

Run:

```bash
cargo test -p brewdb runtime_reads_single_node_aggregate_results_through_exchange -- --nocapture
```

Expected: pass.

