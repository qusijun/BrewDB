//! BrewDB distributed planner contracts.
//!
//! This crate owns the planning boundary between catalog-resolved table
//! bindings and distributed execution plans. It intentionally stays above
//! node-local execution operators and below SQL ingress/frontend concerns.

pub mod distributed;
pub mod errors;
pub mod local;
pub mod logical;
pub use brewdb_common::runtime::QueryContext;
pub use distributed::exchange::{ExchangeNode, ExchangeScope, ExchangeType, PartitioningScheme};
pub use distributed::plan::{
    DistributedPhysicalPlan, PlanFragment, PlanFragmentId, PlanFragmentKind, PlanStageId,
};
pub use distributed::{DistributedPlanner, DistributedPlannerRequest};
pub use errors::PlannerError;
pub use local::LocalFragmentPlan;
pub use logical::plan::{CreateDatabase, Ddl, DropDatabase, LogicalPlanNode, Show};
pub use logical::{
    LogicalOptimizer, LogicalPlanner, LogicalPlanningContext, LogicalPlanningSession,
};

#[cfg(test)]
mod tests {
    use brewdb_catalog::{CatalogMode, LakeFormatKind, TableCatalogEntry, TablePath};
    use brewdb_common::runtime::QueryContext;
    use brewdb_common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use brewdb_sql_parser::dialect::PostgreSqlDialect;
    use brewdb_sql_parser::parser::Parser;
    use datafusion_expr::logical_plan::JoinType as DataFusionJoinType;
    use datafusion_expr::registry::MemoryFunctionRegistry;
    use datafusion_expr::{Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan};
    use datafusion_functions as datafusion_scalar_functions;
    use datafusion_functions_aggregate as datafusion_aggregate_functions;

    use crate::distributed::exchange::{ExchangeScope, ExchangeType, RemoteSourceNode};
    use crate::distributed::plan::{CommandPlan, DistributedPlanRoot, PlanFragmentKind};
    use crate::distributed::{DistributedPlanner, DistributedPlannerRequest};
    use crate::logical::insert::plan_insert_statement;
    use crate::logical::plan::{Ddl, LogicalPlanNode, Show};
    use crate::logical::query::plan_query_statement;

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
                ColumnField::new("id", DataType::Int32),
                ColumnField::new("name", DataType::String),
            ]),
            format!("s3://warehouse/sales/{table_name}"),
            LakeFormatKind::Paimon,
            CatalogMode::Managed,
        )
    }

    fn function_registry() -> MemoryFunctionRegistry {
        let mut registry = MemoryFunctionRegistry::new();
        datafusion_scalar_functions::register_all(&mut registry).unwrap();
        datafusion_aggregate_functions::register_all(&mut registry).unwrap();
        registry
    }

    fn build_query_plan(
        sql: &str,
        tables: Vec<TableCatalogEntry>,
    ) -> crate::distributed::plan::DistributedPhysicalPlan {
        let planner = DistributedPlanner::default();
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0);
        let logical_plan = plan_query_statement(ast, tables, &function_registry()).unwrap();
        planner
            .build(DistributedPlannerRequest {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                logical_plan,
            })
            .unwrap()
    }

    fn build_insert_plan(
        sql: &str,
        target_table: TableCatalogEntry,
        source_tables: Vec<TableCatalogEntry>,
    ) -> crate::distributed::plan::DistributedPhysicalPlan {
        let planner = DistributedPlanner::default();
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0);
        let logical_plan =
            plan_insert_statement(ast, target_table.clone(), &function_registry()).unwrap();
        let _ = source_tables;
        planner
            .build(DistributedPlannerRequest {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                logical_plan,
            })
            .unwrap()
    }

    #[test]
    fn distributed_planner_wraps_scan_into_single_source_fragment() {
        let plan = build_query_plan("select * from orders", vec![make_table("orders")]);

        assert_eq!(plan.fragments.len(), 1);
        assert_eq!(plan.command_tag, "SELECT");
        assert!(plan.returns_rows);
        assert_eq!(plan.table_catalogs.len(), 1);
        assert!(matches!(
            plan.fragments[0].root,
            Some(DataFusionLogicalPlan::TableScan(_))
        ));
        assert_eq!(plan.fragments[0].kind, PlanFragmentKind::Root);
        let local_plan = plan.fragments[0]
            .local_plan
            .as_ref()
            .expect("expected fragment local plan");
        assert!(matches!(local_plan, DataFusionLogicalPlan::TableScan(_)));
        let Some(DataFusionLogicalPlan::TableScan(scan)) = plan.fragments[0].root.as_ref() else {
            panic!("expected table scan root");
        };
        assert_eq!(scan.table_name.table(), "orders");
    }

    #[test]
    fn distributed_planner_supports_constant_query_without_from_clause() {
        let plan = build_query_plan("select 1", vec![]);
        let local_plan = plan.fragments[0]
            .local_plan
            .as_ref()
            .expect("expected fragment local plan");

        assert!(plan.table_catalogs.is_empty());
        assert!(matches!(local_plan, DataFusionLogicalPlan::Projection(_)));
    }

    #[test]
    fn distributed_planner_models_insert_values_as_append_write_with_input_plan() {
        let plan = build_insert_plan(
            "insert into orders values (1, 'a')",
            make_table("orders"),
            vec![],
        );

        assert_eq!(plan.command_tag, "INSERT");
        assert!(!plan.returns_rows);
        assert_eq!(plan.table_catalogs.len(), 1);
        assert_eq!(plan.table_catalogs[0].path.table(), "orders");
        let Some(DataFusionLogicalPlan::Dml(dml)) = &plan.fragments[0].root else {
            panic!("expected DataFusion DML root");
        };
        assert_eq!(dml.table_name.table(), "orders");
        let DataFusionLogicalPlan::Projection(projection) = dml.input.as_ref() else {
            panic!("expected INSERT VALUES input to be projected onto target columns");
        };
        assert!(matches!(
            projection.input.as_ref(),
            DataFusionLogicalPlan::Values(_)
        ));
    }

    #[test]
    fn distributed_planner_models_non_compute_logical_plans_as_commands() {
        let planner = DistributedPlanner::default();
        let query_context = QueryContext {
            query_id: uuid::Uuid::new_v4(),
        };
        let plan = planner
            .build(DistributedPlannerRequest {
                query_context: query_context.clone(),
                logical_plan: datafusion_expr::LogicalPlan::Extension(datafusion_expr::Extension {
                    node: std::sync::Arc::new(LogicalPlanNode::Show(Show::Catalogs)),
                }),
            })
            .unwrap();

        assert_eq!(plan.query_context, query_context);
        assert_eq!(plan.command_tag, "SHOW CATALOGS");
        assert!(plan.returns_rows);
        assert!(plan.fragments.is_empty());
        assert!(plan.exchanges.is_empty());
        assert!(matches!(
            plan.root,
            DistributedPlanRoot::Command(CommandPlan::Extension(LogicalPlanNode::Show(
                Show::Catalogs
            )))
        ));

        let plan = planner
            .build(DistributedPlannerRequest {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                logical_plan: datafusion_expr::LogicalPlan::Extension(datafusion_expr::Extension {
                    node: std::sync::Arc::new(LogicalPlanNode::Ddl(Ddl::CreateDatabase(
                        crate::logical::plan::CreateDatabase {
                            catalog_name: "prod".to_owned(),
                            database_name: "sales".to_owned(),
                        },
                    ))),
                }),
            })
            .unwrap();

        assert_eq!(plan.command_tag, "CREATE DATABASE");
        assert!(!plan.returns_rows);
        assert!(plan.fragments.is_empty());
        assert!(matches!(
            plan.root,
            DistributedPlanRoot::Command(CommandPlan::Extension(LogicalPlanNode::Ddl(
                Ddl::CreateDatabase(_)
            )))
        ));

        let plan = planner
            .build(DistributedPlannerRequest {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                logical_plan: datafusion_expr::LogicalPlan::Ddl(
                    datafusion_expr::DdlStatement::DropTable(datafusion_expr::DropTable {
                        name: datafusion_common::TableReference::full("prod", "sales", "orders"),
                        if_exists: false,
                        schema: std::sync::Arc::new(datafusion_common::DFSchema::empty()),
                    }),
                ),
            })
            .unwrap();

        assert_eq!(plan.command_tag, "DROP TABLE");
        assert!(!plan.returns_rows);
        assert!(plan.fragments.is_empty());
        assert!(matches!(
            plan.root,
            DistributedPlanRoot::Command(CommandPlan::Ddl(
                datafusion_expr::DdlStatement::DropTable(_)
            ))
        ));
    }

    #[test]
    fn distributed_planner_resolves_datafusion_function_expr() {
        let plan = build_query_plan("select lower(name) from orders", vec![make_table("orders")]);
        let local_plan = plan.fragments[0]
            .local_plan
            .as_ref()
            .expect("expected worker-facing fragment plan");
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
        let root = plan.fragments[0].root.as_ref().expect("expected root plan");
        let Some(scan) = find_table_scan(root) else {
            panic!("expected table scan in query plan");
        };
        assert_eq!(scan.table_name.table(), "orders");
        assert!(scan.projection.is_some());
        assert!(matches!(root, DataFusionLogicalPlan::Filter(_)));
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
        let DataFusionLogicalPlan::Aggregate(aggregate) =
            plan.fragments[0].root.as_ref().expect("expected root plan")
        else {
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
        let Some(DataFusionLogicalPlan::Join(join)) = &plan.fragments[0].root else {
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
        let Some(DataFusionLogicalPlan::Join(join)) = &plan.fragments[0].root else {
            panic!("expected join input");
        };
        assert!(join.on.len() <= 1);
    }
}
