//! BrewDB distributed planner contracts.
//!
//! This crate owns the planning boundary between catalog-resolved table
//! bindings and distributed execution plans. It intentionally stays above
//! node-local execution operators and below SQL ingress/frontend concerns.

pub mod distributed;
pub mod errors;
pub mod exchange;
pub mod local;
pub mod logical;
pub mod plan;

pub use brewdb_common::runtime::QueryContext;
pub use distributed::{DistributedPlanner, DistributedPlannerRequest};
pub use errors::PlannerError;
pub use exchange::{ExchangeNode, ExchangeScope, ExchangeType, PartitioningScheme};
pub use local::LocalFragmentPlan;
pub use logical::{
    DeletePlanRoot, InsertPlanRoot, LogicalPlan, MergePlanRoot, QueryExpression, QueryGroupBy,
    QueryPlanRoot, UpdatePlanRoot,
};
pub use plan::{PlanFragment, PlanFragmentId, PlanFragmentKind, PlanStageId};

#[cfg(test)]
mod tests {
    use brewdb_catalog::{CatalogMode, LakeFormatKind, TableCatalogEntry, TablePath};
    use brewdb_common::runtime::QueryContext;
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};
    use brewdb_sql::{BoundPlanStatement, BoundQueryStatement, BoundSessionContext};
    use datafusion_expr::logical_plan::JoinType as DataFusionJoinType;
    use datafusion_expr::{Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan};
    use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion_sql::sqlparser::parser::Parser;

    use crate::distributed::{DistributedPlanner, DistributedPlannerRequest};
    use crate::exchange::{ExchangeScope, ExchangeType, RemoteSourceNode};
    use crate::logical::{LogicalPlan, QueryGroupBy};
    use crate::plan::PlanFragmentKind;

    fn find_table_scan<'a>(
        plan: &'a DataFusionLogicalPlan,
    ) -> Option<&'a datafusion_expr::TableScan> {
        match plan {
            DataFusionLogicalPlan::TableScan(scan) => Some(scan),
            _ => plan.inputs().into_iter().find_map(find_table_scan),
        }
    }

    fn make_table(table_name: &str) -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", table_name).unwrap(),
            TableSchema::new(vec![
                SchemaField::new("id", DataType::Int32),
                SchemaField::new("name", DataType::String),
            ]),
            format!("s3://warehouse/sales/{table_name}"),
            LakeFormatKind::Paimon,
            CatalogMode::Managed,
        )
    }

    fn build_query_plan(sql: &str, tables: Vec<TableCatalogEntry>) -> crate::plan::DistributedPlan {
        let planner = DistributedPlanner::default();
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0);
        planner
            .build(DistributedPlannerRequest {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                statement: BoundPlanStatement::Query(BoundQueryStatement {
                    statement_text: sql.to_owned(),
                    session: BoundSessionContext {
                        session_id: uuid::Uuid::new_v4(),
                        user_name: "brew".to_owned(),
                        catalog_name: "prod".to_owned(),
                        database_name: "sales".to_owned(),
                    },
                    tables,
                    ast,
                }),
            })
            .unwrap()
    }

    #[test]
    fn distributed_planner_wraps_scan_into_single_source_fragment() {
        let plan = build_query_plan("select * from orders", vec![make_table("orders")]);

        assert_eq!(plan.fragments.len(), 1);
        assert!(matches!(
            plan.fragments[0].root,
            Some(LogicalPlan::Query(_))
        ));
        assert_eq!(plan.fragments[0].kind, PlanFragmentKind::Root);
        let local_plan = plan.fragments[0]
            .local_plan
            .as_ref()
            .expect("expected fragment local plan");
        let Some(LogicalPlan::Query(query_root)) = &plan.fragments[0].root else {
            panic!("expected query root");
        };
        assert_eq!(query_root.query.projection.len(), 1);
        assert!(matches!(query_root.query.group_by, QueryGroupBy::None));
        assert!(matches!(local_plan, DataFusionLogicalPlan::TableScan(_)));
        let Some(DataFusionLogicalPlan::TableScan(scan)) = query_root.input.as_ref() else {
            panic!("expected table scan root");
        };
        assert_eq!(scan.table_name.table(), "orders");
    }

    #[test]
    fn distributed_planner_resolves_datafusion_function_expr() {
        let plan = build_query_plan("select lower(name) from orders", vec![make_table("orders")]);
        let Some(LogicalPlan::Query(query_root)) = &plan.fragments[0].root else {
            panic!("expected query root");
        };
        let local_plan = plan.fragments[0]
            .local_plan
            .as_ref()
            .expect("expected worker-facing fragment plan");
        assert!(matches!(
            query_root.query.projection.first(),
            Some(DataFusionExpr::ScalarFunction(_))
        ));
        let DataFusionLogicalPlan::Projection(projection) = local_plan else {
            panic!("expected projection root");
        };
        assert_eq!(projection.expr.len(), 1);
        assert!(matches!(
            projection.expr[0],
            DataFusionExpr::ScalarFunction(_)
        ));
    }

    #[test]
    fn distributed_planner_builds_filter_projection_tree() {
        let plan = build_query_plan(
            "select id from orders where id > 10",
            vec![make_table("orders")],
        );
        let Some(LogicalPlan::Query(query_root)) = &plan.fragments[0].root else {
            panic!("expected query root");
        };
        let Some(scan) = find_table_scan(query_root.input.as_ref().unwrap()) else {
            panic!("expected table scan in query plan");
        };
        assert_eq!(scan.table_name.table(), "orders");
        assert!(scan.projection.is_some());
        assert!(matches!(
            query_root.input.as_ref().unwrap(),
            DataFusionLogicalPlan::Filter(_)
        ));
    }

    #[test]
    fn distributed_planner_builds_aggregate_tree() {
        let plan = build_query_plan("select count(id) from orders", vec![make_table("orders")]);
        assert_eq!(plan.fragments.len(), 2);
        assert_eq!(plan.exchanges.len(), 1);
        assert_eq!(plan.exchanges[0].scope, ExchangeScope::Remote);
        assert_eq!(plan.exchanges[0].exchange_type, ExchangeType::Gather);
        assert!(
            plan.exchanges[0]
                .partitioning_scheme
                .partition_keys
                .is_empty()
        );
        assert_eq!(plan.exchanges[0].partitioning_scheme.output_layout.len(), 1);
        assert_eq!(plan.fragments[1].kind, PlanFragmentKind::Source);
        let Some(LogicalPlan::Query(query_root)) = &plan.fragments[0].root else {
            panic!("expected query root");
        };
        let DataFusionLogicalPlan::Aggregate(aggregate) = query_root.input.as_ref().unwrap() else {
            panic!("expected aggregate root");
        };
        assert_eq!(aggregate.aggr_expr.len(), 1);
        assert!(matches!(
            aggregate.aggr_expr[0],
            DataFusionExpr::AggregateFunction(_)
        ));
        let DataFusionLogicalPlan::Extension(extension) = aggregate.input.as_ref() else {
            panic!("expected exchange placeholder input");
        };
        assert!(
            extension
                .node
                .as_any()
                .downcast_ref::<RemoteSourceNode>()
                .is_some()
        );
        let Some(DataFusionLogicalPlan::TableScan(scan)) = plan.fragments[1].local_plan.as_ref()
        else {
            panic!("expected scan local plan in child fragment");
        };
        assert_eq!(scan.table_name.table(), "orders");
    }

    #[test]
    fn distributed_planner_builds_join_tree() {
        let plan = build_query_plan(
            "select * from orders o join customers c on o.id = c.id and o.id > 10",
            vec![make_table("orders"), make_table("customers")],
        );
        assert_eq!(plan.fragments.len(), 3);
        assert_eq!(plan.exchanges.len(), 2);
        eprintln!("non_column plan = {:#?}", plan.exchanges);
        eprintln!("non_column exchanges = {:#?}", plan.exchanges);
        assert!(
            plan.exchanges
                .iter()
                .all(|edge| edge.scope == ExchangeScope::Remote)
        );
        assert!(
            plan.exchanges
                .iter()
                .all(|edge| edge.exchange_type == ExchangeType::Repartition)
        );
        assert!(
            plan.exchanges
                .iter()
                .all(|edge| edge.partitioning_scheme.partition_keys.len() == 1)
        );
        assert!(
            plan.exchanges
                .iter()
                .all(|edge| !edge.partitioning_scheme.output_layout.is_empty())
        );
        assert!(
            plan.fragments[1..]
                .iter()
                .all(|fragment| fragment.kind == PlanFragmentKind::Source)
        );
        let Some(LogicalPlan::Query(query_root)) = &plan.fragments[0].root else {
            panic!("expected query root");
        };
        let Some(DataFusionLogicalPlan::Join(join)) = query_root.input.as_ref() else {
            panic!("expected join input");
        };
        assert_eq!(join.join_type, DataFusionJoinType::Inner);
        assert_eq!(join.on.len(), 1);
        assert!(join.filter.is_none());
        let DataFusionExpr::Column(left_key) = &join.on[0].0 else {
            panic!("expected left join key column");
        };
        let DataFusionExpr::Column(right_key) = &join.on[0].1 else {
            panic!("expected right join key column");
        };
        assert_eq!(
            left_key
                .relation
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("o")
        );
        assert_eq!(left_key.name, "id");
        assert_eq!(
            right_key
                .relation
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("c")
        );
        assert_eq!(right_key.name, "id");
        let DataFusionLogicalPlan::Extension(left_exchange) = join.left.as_ref() else {
            panic!("expected left exchange placeholder");
        };
        assert!(
            left_exchange
                .node
                .as_any()
                .downcast_ref::<RemoteSourceNode>()
                .is_some()
        );
        let DataFusionLogicalPlan::Extension(right_exchange) = join.right.as_ref() else {
            panic!("expected right exchange placeholder");
        };
        assert!(
            right_exchange
                .node
                .as_any()
                .downcast_ref::<RemoteSourceNode>()
                .is_some()
        );
        let Some(left_scan) = find_table_scan(plan.fragments[1].local_plan.as_ref().unwrap())
        else {
            panic!("expected left scan child fragment");
        };
        let Some(right_scan) = find_table_scan(plan.fragments[2].local_plan.as_ref().unwrap())
        else {
            panic!("expected right scan child fragment");
        };
        assert_eq!(left_scan.table_name.table(), "o");
        assert_eq!(right_scan.table_name.table(), "c");
    }

    #[test]
    fn distributed_planner_keeps_non_column_equality_in_join_filter() {
        let plan = build_query_plan(
            "select * from orders o join customers c on lower(o.name) = lower(c.name)",
            vec![make_table("orders"), make_table("customers")],
        );
        assert_eq!(plan.fragments.len(), 3);
        assert_eq!(plan.exchanges.len(), 2);
        assert!(
            plan.exchanges
                .iter()
                .all(|edge| edge.scope == ExchangeScope::Remote)
        );
        assert!(
            plan.exchanges
                .iter()
                .all(|edge| !edge.partitioning_scheme.output_layout.is_empty())
        );
        let Some(LogicalPlan::Query(query_root)) = &plan.fragments[0].root else {
            panic!("expected query root");
        };
        let Some(DataFusionLogicalPlan::Join(join)) = query_root.input.as_ref() else {
            panic!("expected join input");
        };
        assert!(join.on.len() <= 1);
    }
}
