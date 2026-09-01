#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use crate::catalog::{CatalogMode, StorageKind, TableCatalogEntry, TablePath};
    use crate::common::context::QueryContext;
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::parser::dialect::PostgreSqlDialect;
    use crate::parser::Parser;
    use brewdb_common::test_util::TestFile;
    use datafusion_common::TableReference;
    use datafusion_expr::logical_plan::dml::{DmlStatement, InsertOp, WriteOp};
    use datafusion_expr::logical_plan::JoinType as DataFusionJoinType;
    use datafusion_expr::registry::MemoryFunctionRegistry;
    use datafusion_expr::{
        Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder,
        TableSource,
    };
    use datafusion_functions as datafusion_scalar_functions;
    use datafusion_functions_aggregate as datafusion_aggregate_functions;

    use crate::planner::distributed::exchange::{ExchangeScope, ExchangeType, RemoteSourceNode};
    use crate::planner::distributed::{DistributedFragmentPlanner, FragmentPlanner};
    use crate::planner::distributed::{DistributedPlanRoot, PlanFragmentKind};
    use crate::planner::logical::mutation::plan_insert_statement;
    use crate::planner::logical::optimizer::LogicalOptimizer;
    use crate::planner::logical::plan::{Ddl, LogicalPlanNode, Show};
    use crate::planner::logical::query::plan_query_statement;
    use crate::planner::logical::table_source::DefaultTableSource;
    use crate::planner::{CommandPlan, CommandTag};
    use crate::storage::{open_storage_engine, TableScanSplit, TableScanSplitGroup};

    fn find_table_scan<'a>(
        plan: &'a DataFusionLogicalPlan,
    ) -> Option<&'a datafusion_expr::TableScan> {
        match plan {
            DataFusionLogicalPlan::TableScan(scan) => Some(scan),
            _ => plan.inputs().into_iter().find_map(find_table_scan),
        }
    }

    fn find_aggregate<'a>(
        plan: &'a DataFusionLogicalPlan,
    ) -> Option<&'a datafusion_expr::Aggregate> {
        match plan {
            DataFusionLogicalPlan::Aggregate(aggregate) => Some(aggregate),
            _ => plan.inputs().into_iter().find_map(find_aggregate),
        }
    }

    fn find_filter<'a>(plan: &'a DataFusionLogicalPlan) -> Option<&'a datafusion_expr::Filter> {
        match plan {
            DataFusionLogicalPlan::Filter(filter) => Some(filter),
            _ => plan.inputs().into_iter().find_map(find_filter),
        }
    }

    fn find_projection<'a>(
        plan: &'a DataFusionLogicalPlan,
    ) -> Option<&'a datafusion_expr::Projection> {
        match plan {
            DataFusionLogicalPlan::Projection(projection) => Some(projection),
            _ => plan.inputs().into_iter().find_map(find_projection),
        }
    }

    fn find_distinct<'a>(
        plan: &'a DataFusionLogicalPlan,
    ) -> Option<&'a datafusion_expr::logical_plan::Distinct> {
        match plan {
            DataFusionLogicalPlan::Distinct(distinct) => Some(distinct),
            _ => plan.inputs().into_iter().find_map(find_distinct),
        }
    }

    fn find_sort<'a>(plan: &'a DataFusionLogicalPlan) -> Option<&'a datafusion_expr::Sort> {
        match plan {
            DataFusionLogicalPlan::Sort(sort) => Some(sort),
            _ => plan.inputs().into_iter().find_map(find_sort),
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
            StorageKind::Paimon,
            CatalogMode::Managed,
        )
    }

    fn default_table_source(table: TableCatalogEntry) -> DefaultTableSource {
        let engine = open_storage_engine().unwrap().table_engine(&table).unwrap();
        DefaultTableSource::new(table, engine)
    }

    fn make_hits_table() -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "hits").unwrap(),
            TableSchema::new(vec![
                ColumnField::new("UserID", DataType::Int64),
                ColumnField::new("URL", DataType::String),
            ]),
            "s3://warehouse/sales/hits",
            StorageKind::Paimon,
            CatalogMode::Managed,
        )
    }

    fn make_clickbench_hits_table() -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "hits").unwrap(),
            TableSchema::new(vec![
                ColumnField::new("WatchID", DataType::Int64),
                ColumnField::new("JavaEnable", DataType::Int16),
                ColumnField::new("Title", DataType::String),
                ColumnField::new("GoodEvent", DataType::Int32),
                ColumnField::new("EventTime", DataType::String),
                ColumnField::new("EventDate", DataType::Date),
                ColumnField::new("CounterID", DataType::Int32),
                ColumnField::new("ClientIP", DataType::Int64),
                ColumnField::new("RegionID", DataType::Int32),
                ColumnField::new("UserID", DataType::Int64),
                ColumnField::new("CounterClass", DataType::Int16),
                ColumnField::new("OS", DataType::Int16),
                ColumnField::new("UserAgent", DataType::Int16),
                ColumnField::new("URL", DataType::String),
                ColumnField::new("Referer", DataType::String),
                ColumnField::new("IsRefresh", DataType::Int16),
                ColumnField::new("RefererCategoryID", DataType::Int16),
                ColumnField::new("RefererRegionID", DataType::Int32),
                ColumnField::new("URLCategoryID", DataType::Int16),
                ColumnField::new("URLRegionID", DataType::Int32),
                ColumnField::new("ResolutionWidth", DataType::Int16),
                ColumnField::new("ResolutionHeight", DataType::Int16),
                ColumnField::new("ResolutionDepth", DataType::Int16),
                ColumnField::new("FlashMajor", DataType::Int16),
                ColumnField::new("FlashMinor", DataType::Int16),
                ColumnField::new("FlashMinor2", DataType::String),
                ColumnField::new("NetMajor", DataType::Int16),
                ColumnField::new("NetMinor", DataType::Int16),
                ColumnField::new("UserAgentMajor", DataType::Int16),
                ColumnField::new("UserAgentMinor", DataType::String),
                ColumnField::new("CookieEnable", DataType::Int16),
                ColumnField::new("JavascriptEnable", DataType::Int16),
                ColumnField::new("IsMobile", DataType::Int16),
                ColumnField::new("MobilePhone", DataType::Int16),
                ColumnField::new("MobilePhoneModel", DataType::String),
                ColumnField::new("Params", DataType::String),
                ColumnField::new("IPNetworkID", DataType::Int32),
                ColumnField::new("TraficSourceID", DataType::Int16),
                ColumnField::new("SearchEngineID", DataType::Int16),
                ColumnField::new("SearchPhrase", DataType::String),
                ColumnField::new("AdvEngineID", DataType::Int16),
                ColumnField::new("IsArtifical", DataType::Int16),
                ColumnField::new("WindowClientWidth", DataType::Int16),
                ColumnField::new("WindowClientHeight", DataType::Int16),
                ColumnField::new("ClientTimeZone", DataType::Int16),
                ColumnField::new("ClientEventTime", DataType::String),
                ColumnField::new("SilverlightVersion1", DataType::Int16),
                ColumnField::new("SilverlightVersion2", DataType::Int16),
                ColumnField::new("SilverlightVersion3", DataType::Int32),
                ColumnField::new("SilverlightVersion4", DataType::Int16),
                ColumnField::new("PageCharset", DataType::String),
                ColumnField::new("CodeVersion", DataType::Int32),
                ColumnField::new("IsLink", DataType::Int16),
                ColumnField::new("IsDownload", DataType::Int16),
                ColumnField::new("IsNotBounce", DataType::Int16),
                ColumnField::new("FUniqID", DataType::Int64),
                ColumnField::new("OriginalURL", DataType::String),
                ColumnField::new("HID", DataType::Int32),
                ColumnField::new("IsOldCounter", DataType::Int16),
                ColumnField::new("IsEvent", DataType::Int16),
                ColumnField::new("IsParameter", DataType::Int16),
                ColumnField::new("DontCountHits", DataType::Int16),
                ColumnField::new("WithHash", DataType::Int16),
                ColumnField::new("HitColor", DataType::String),
                ColumnField::new("LocalEventTime", DataType::String),
                ColumnField::new("Age", DataType::Int16),
                ColumnField::new("Sex", DataType::Int16),
                ColumnField::new("Income", DataType::Int16),
                ColumnField::new("Interests", DataType::Int16),
                ColumnField::new("Robotness", DataType::Int16),
                ColumnField::new("RemoteIP", DataType::Int64),
                ColumnField::new("WindowName", DataType::Int32),
                ColumnField::new("OpenerName", DataType::Int32),
                ColumnField::new("HistoryLength", DataType::Int16),
                ColumnField::new("BrowserLanguage", DataType::String),
                ColumnField::new("BrowserCountry", DataType::String),
                ColumnField::new("SocialNetwork", DataType::String),
                ColumnField::new("SocialAction", DataType::String),
                ColumnField::new("HTTPError", DataType::Int16),
                ColumnField::new("SendTiming", DataType::Int32),
                ColumnField::new("DNSTiming", DataType::Int32),
                ColumnField::new("ConnectTiming", DataType::Int32),
                ColumnField::new("ResponseStartTiming", DataType::Int32),
                ColumnField::new("ResponseEndTiming", DataType::Int32),
                ColumnField::new("FetchTiming", DataType::Int32),
                ColumnField::new("SocialSourceNetworkID", DataType::Int16),
                ColumnField::new("SocialSourcePage", DataType::String),
                ColumnField::new("ParamPrice", DataType::Int64),
                ColumnField::new("ParamOrderID", DataType::String),
                ColumnField::new("ParamCurrency", DataType::String),
                ColumnField::new("ParamCurrencyID", DataType::Int16),
                ColumnField::new("OpenstatServiceName", DataType::String),
                ColumnField::new("OpenstatCampaignID", DataType::String),
                ColumnField::new("OpenstatAdID", DataType::String),
                ColumnField::new("OpenstatSourceID", DataType::String),
                ColumnField::new("UTMSource", DataType::String),
                ColumnField::new("UTMMedium", DataType::String),
                ColumnField::new("UTMCampaign", DataType::String),
                ColumnField::new("UTMContent", DataType::String),
                ColumnField::new("UTMTerm", DataType::String),
                ColumnField::new("FromTag", DataType::String),
                ColumnField::new("HasGCLID", DataType::Int16),
                ColumnField::new("RefererHash", DataType::Int64),
                ColumnField::new("URLHash", DataType::Int64),
                ColumnField::new("CLID", DataType::Int32),
            ]),
            "s3://warehouse/sales/hits",
            StorageKind::Paimon,
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
    ) -> crate::planner::distributed::DistributedFragmentPlan {
        let planner = DistributedFragmentPlanner::default();
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0);
        let logical_plan = plan_query_statement(ast, tables, &function_registry()).unwrap();
        planner
            .build(
                QueryContext::for_test(uuid::Uuid::new_v4()),
                logical_plan,
                crate::storage::open_storage_engine().unwrap(),
            )
            .unwrap()
    }

    fn build_insert_plan(
        sql: &str,
        target_table: TableCatalogEntry,
        source_tables: Vec<TableCatalogEntry>,
    ) -> crate::planner::distributed::DistributedFragmentPlan {
        let planner = DistributedFragmentPlanner::default();
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0);
        let logical_plan =
            plan_insert_statement(ast, target_table.clone(), &function_registry()).unwrap();
        let _ = source_tables;
        planner
            .build(
                QueryContext::for_test(uuid::Uuid::new_v4()),
                logical_plan,
                crate::storage::open_storage_engine().unwrap(),
            )
            .unwrap()
    }

    #[test]
    fn distributed_fragment_planner_wraps_scan_into_single_source_fragment() {
        let plan = build_query_plan("select * from orders", vec![make_table("orders")]);

        assert_eq!(plan.fragments.len(), 1);
        assert_eq!(plan.command_tag, CommandTag::Select);
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
        assert_eq!(
            plan.table_scan_splits,
            TableScanSplitGroup::new(vec![TableScanSplit::new("orders", 0)])
        );
    }

    #[test]
    fn default_table_source_exposes_primary_key_constraints_to_datafusion_scan() {
        use datafusion_common::{Constraint, Constraints, FunctionalDependencies};

        let mut table = make_table("orders");
        table.table_schema.primary_keys = vec!["id".to_owned()];

        let source: Arc<dyn TableSource> = Arc::new(default_table_source(table));
        let plan = LogicalPlanBuilder::scan("orders", source, None)
            .unwrap()
            .build()
            .unwrap();
        let scan = find_table_scan(&plan).unwrap();

        assert_eq!(
            scan.source.constraints(),
            Some(&Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![0]
            )]))
        );
        assert_ne!(
            scan.projected_schema.functional_dependencies(),
            &FunctionalDependencies::empty()
        );
    }

    #[test]
    fn logical_optimizer_pushes_inexact_paimon_filter_into_table_scan() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select id from orders where name = 'latte'",
        )
        .unwrap()
        .remove(0);
        let planned =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();
        let optimized = LogicalOptimizer::default().optimize(planned).unwrap();
        let scan = find_table_scan(&optimized).expect("expected table scan");

        assert_eq!(scan.filters.len(), 1);
        assert!(
            find_filter(&optimized).is_some(),
            "inexact Paimon filters need residual filtering"
        );
    }

    #[test]
    fn logical_optimizer_pushes_exact_paimon_partition_filter_into_table_scan() {
        let mut table = make_table("orders");
        table.table_schema.partition_keys = vec!["name".to_owned()];
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select id from orders where name = 'latte'",
        )
        .unwrap()
        .remove(0);
        let planned = plan_query_statement(ast, vec![table], &function_registry()).unwrap();
        let optimized = LogicalOptimizer::default().optimize(planned).unwrap();
        let scan = find_table_scan(&optimized).expect("expected table scan");

        assert_eq!(scan.filters.len(), 1);
        assert!(
            find_filter(&optimized).is_none(),
            "exact partition filters should not leave residual filtering"
        );
    }

    #[test]
    fn distributed_fragment_planner_collects_file_table_engine_split_candidates() {
        let path = TestFile::new("brewdb-file-split", "csv");
        std::fs::write(path.path(), "id\n1\n").unwrap();

        let target_table = make_table("orders");
        let source_name = "__copy_from_orders";
        let source_table = TableCatalogEntry::temporary_file(
            source_name,
            path.path().to_string_lossy().to_string(),
            [("format", "csv"), ("has_header", "true")],
        )
        .unwrap();
        let source: Arc<dyn TableSource> = Arc::new(default_table_source(source_table));
        let input = LogicalPlanBuilder::scan(source_name, source, None)
            .unwrap()
            .build()
            .unwrap();
        let target: Arc<dyn TableSource> = Arc::new(default_table_source(target_table.clone()));
        let logical_plan = DataFusionLogicalPlan::Dml(DmlStatement::new(
            TableReference::full(
                target_table.path.catalog(),
                target_table.path.database(),
                target_table.path.table(),
            ),
            target,
            WriteOp::Insert(InsertOp::Append),
            Arc::new(input),
        ));

        let plan = DistributedFragmentPlanner::default()
            .build(
                QueryContext::for_test(uuid::Uuid::new_v4()),
                logical_plan,
                crate::storage::open_storage_engine().unwrap(),
            )
            .unwrap();

        assert_eq!(plan.table_scan_splits.len(), 1);
        let splits = plan.table_scan_splits.only_table_source_splits().unwrap();
        assert_eq!(splits[0].table_name, "__copy_from_orders");
        assert!(
            splits[0].locations[0].ends_with(path.path().file_name().unwrap().to_str().unwrap())
        );
    }

    #[test]
    fn distributed_fragment_planner_supports_constant_query_without_from_clause() {
        let plan = build_query_plan("select 1", vec![]);
        let local_plan = plan.fragments[0]
            .local_plan
            .as_ref()
            .expect("expected fragment local plan");

        assert!(plan.table_catalogs.is_empty());
        assert!(matches!(local_plan, DataFusionLogicalPlan::Projection(_)));
    }

    #[test]
    fn distributed_fragment_planner_models_insert_values_as_append_write_with_input_plan() {
        let plan = build_insert_plan(
            "insert into orders values (1, 'a')",
            make_table("orders"),
            vec![],
        );

        assert_eq!(plan.command_tag, CommandTag::Insert);
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
    fn distributed_fragment_planner_models_non_compute_logical_plans_as_commands() {
        let planner = DistributedFragmentPlanner::default();
        let query_context = QueryContext::for_test(uuid::Uuid::new_v4());
        let plan = planner
            .build(
                query_context.clone(),
                datafusion_expr::LogicalPlan::Extension(datafusion_expr::Extension {
                    node: std::sync::Arc::new(LogicalPlanNode::Show(Show::Catalogs)),
                }),
                crate::storage::open_storage_engine().unwrap(),
            )
            .unwrap();

        assert_eq!(plan.query_context, query_context);
        assert_eq!(plan.command_tag, CommandTag::ShowCatalogs);
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
            .build(
                QueryContext::for_test(uuid::Uuid::new_v4()),
                datafusion_expr::LogicalPlan::Extension(datafusion_expr::Extension {
                    node: std::sync::Arc::new(LogicalPlanNode::Ddl(Ddl::CreateDatabase(
                        crate::planner::logical::plan::CreateDatabase {
                            catalog_name: "prod".to_owned(),
                            database_name: "sales".to_owned(),
                        },
                    ))),
                }),
                crate::storage::open_storage_engine().unwrap(),
            )
            .unwrap();

        assert_eq!(plan.command_tag, CommandTag::CreateDatabase);
        assert!(!plan.returns_rows);
        assert!(plan.fragments.is_empty());
        assert!(matches!(
            plan.root,
            DistributedPlanRoot::Command(CommandPlan::Extension(LogicalPlanNode::Ddl(
                Ddl::CreateDatabase(_)
            )))
        ));

        let plan = planner
            .build(
                QueryContext::for_test(uuid::Uuid::new_v4()),
                datafusion_expr::LogicalPlan::Ddl(datafusion_expr::DdlStatement::DropTable(
                    datafusion_expr::DropTable {
                        name: datafusion_common::TableReference::full("prod", "sales", "orders"),
                        if_exists: false,
                        schema: std::sync::Arc::new(datafusion_common::DFSchema::empty()),
                    },
                )),
                crate::storage::open_storage_engine().unwrap(),
            )
            .unwrap();

        assert_eq!(plan.command_tag, CommandTag::DropTable);
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
    fn distributed_fragment_planner_resolves_datafusion_function_expr() {
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
    fn distributed_fragment_planner_builds_filter_projection_tree() {
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
    fn distributed_fragment_planner_builds_aggregate_tree() {
        let plan = build_query_plan("select count(id) from orders", vec![make_table("orders")]);
        assert_eq!(plan.fragments.len(), 2);
        assert_eq!(plan.exchanges.len(), 1);
        assert_eq!(plan.exchanges[0].scope, ExchangeScope::Remote);
        assert_eq!(plan.exchanges[0].exchange_type, ExchangeType::Gather);
        assert!(plan.exchanges[0]
            .partitioning_scheme
            .partition_keys
            .is_empty());
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
        assert!(extension
            .node
            .as_any()
            .downcast_ref::<RemoteSourceNode>()
            .is_some());
        let Some(DataFusionLogicalPlan::TableScan(scan)) = plan.fragments[1].local_plan.as_ref()
        else {
            panic!("expected scan local plan in child fragment");
        };
        assert_eq!(scan.table_name.table(), "orders");
    }

    #[test]
    fn logical_planner_resolves_uppercase_aggregate_function_names() {
        let plan = build_query_plan("select COUNT(*) from hits", vec![make_hits_table()]);
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");
        assert_eq!(aggregate.aggr_expr.len(), 1);
    }

    #[test]
    fn logical_planner_resolves_unquoted_columns_case_insensitively() {
        let plan = build_query_plan(
            "select userid from hits where url = 'https://example.com'",
            vec![make_hits_table()],
        );
        let filter =
            find_filter(plan.fragments[0].root.as_ref().unwrap()).expect("expected filter plan");
        let DataFusionExpr::BinaryExpr(binary) = &filter.predicate else {
            panic!("expected binary predicate");
        };
        let DataFusionExpr::Column(column) = binary.left.as_ref() else {
            panic!("expected column predicate");
        };
        assert_eq!(column.name, "URL");
    }

    #[test]
    fn logical_planner_maps_clickbench_length_function_alias() {
        let plan = build_query_plan("select length(url) from hits", vec![make_hits_table()]);
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
    fn logical_planner_resolves_datafusion_scalar_function_aliases() {
        let plan = build_query_plan("select char_length(url) from hits", vec![make_hits_table()]);
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
    fn logical_planner_plans_extract_without_registry_lookup() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select extract(year from created_at) from orders",
        )
        .unwrap()
        .remove(0);
        let orders = TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("created_at", DataType::Date)]),
            "s3://warehouse/sales/orders",
            StorageKind::Paimon,
            CatalogMode::Managed,
        );
        let logical_plan =
            plan_query_statement(ast, vec![orders], &MemoryFunctionRegistry::new()).unwrap();
        let projection = find_projection(&logical_plan).expect("expected projection plan");

        assert!(matches!(
            projection.expr[0],
            DataFusionExpr::ScalarFunction(_)
        ));
    }

    #[test]
    fn logical_planner_plans_substring_without_registry_lookup() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select substring(name from 1 for 2) from orders",
        )
        .unwrap()
        .remove(0);
        let logical_plan = plan_query_statement(
            ast,
            vec![make_table("orders")],
            &MemoryFunctionRegistry::new(),
        )
        .unwrap();
        let projection = find_projection(&logical_plan).expect("expected projection plan");

        assert!(matches!(
            projection.expr[0],
            DataFusionExpr::ScalarFunction(_)
        ));
    }

    #[test]
    fn logical_planner_builds_full_table_reference_for_fully_qualified_scan() {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "select id from prod.sales.orders")
            .unwrap()
            .remove(0);
        let logical_plan =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();
        let scan = find_table_scan(&logical_plan).expect("expected table scan");

        assert_eq!(
            scan.table_name,
            TableReference::full("prod", "sales", "orders")
        );
    }

    #[test]
    fn logical_planner_builds_partial_table_reference_for_schema_qualified_scan() {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "select id from sales.orders")
            .unwrap()
            .remove(0);
        let logical_plan =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();
        let scan = find_table_scan(&logical_plan).expect("expected table scan");

        assert_eq!(scan.table_name, TableReference::partial("sales", "orders"));
    }

    #[test]
    fn logical_planner_builds_bare_table_reference_for_scan_alias() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select o.id from prod.sales.orders o",
        )
        .unwrap()
        .remove(0);
        let logical_plan =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();
        let scan = find_table_scan(&logical_plan).expect("expected table scan");

        assert_eq!(scan.table_name, TableReference::bare("o"));
    }

    #[test]
    fn logical_planner_uses_table_alias_as_column_qualifier() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select id from orders o where o.name = 'latte'",
        )
        .unwrap()
        .remove(0);
        let logical_plan =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();
        let filter = find_filter(&logical_plan).expect("expected filter plan");
        let DataFusionExpr::BinaryExpr(binary) = &filter.predicate else {
            panic!("expected binary predicate");
        };
        let DataFusionExpr::Column(column) = binary.left.as_ref() else {
            panic!("expected column predicate");
        };
        assert_eq!(column.relation, Some(TableReference::bare("o")));
        assert_eq!(column.name, "name");
    }

    #[test]
    fn logical_planner_uses_derived_table_alias_as_column_qualifier() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select d.order_id from (select id as order_id from orders) d",
        )
        .unwrap()
        .remove(0);
        let logical_plan =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();
        let projection = find_projection(&logical_plan).expect("expected projection plan");
        let DataFusionExpr::Column(column) = &projection.expr[0] else {
            panic!("expected projected column");
        };
        assert_eq!(column.relation, Some(TableReference::bare("d")));
        assert_eq!(column.name, "order_id");
    }

    #[test]
    fn logical_planner_expands_qualified_wildcard_with_table_alias() {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "select o.* from orders o")
            .unwrap()
            .remove(0);
        let logical_plan =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();
        let projection = find_projection(&logical_plan).expect("expected projection plan");

        assert_eq!(projection.expr.len(), 2);
        for expr in &projection.expr {
            let DataFusionExpr::Column(column) = expr else {
                panic!("expected expanded wildcard column");
            };
            assert_eq!(column.relation, Some(TableReference::bare("o")));
        }
    }

    #[test]
    fn logical_planner_expands_fully_qualified_wildcard() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select prod.sales.orders.* from prod.sales.orders",
        )
        .unwrap()
        .remove(0);
        let logical_plan =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();
        let projection = find_projection(&logical_plan).expect("expected projection plan");

        assert_eq!(projection.expr.len(), 2);
        for expr in &projection.expr {
            let DataFusionExpr::Column(column) = expr else {
                panic!("expected expanded wildcard column");
            };
            assert_eq!(
                column.relation,
                Some(TableReference::full("prod", "sales", "orders"))
            );
        }
    }

    #[test]
    fn logical_planner_rebases_aggregate_with_function_group_key() {
        let plan = build_query_plan(
            "select regexp_replace(referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') as k, avg(length(referer)) as l, count(*) as c, min(referer) from hits where referer <> '' group by k having count(*) > 100000 order by l desc limit 25",
            vec![TableCatalogEntry::new(
                uuid::Uuid::new_v4(),
                uuid::Uuid::new_v4(),
                uuid::Uuid::new_v4(),
                TablePath::new("prod", "sales", "hits").unwrap(),
                TableSchema::new(vec![
                    ColumnField::new("Referer", DataType::String),
                ]),
                "s3://warehouse/sales/hits",
                StorageKind::Paimon,
                CatalogMode::Managed,
            )],
        );
        assert!(find_sort(plan.fragments[0].root.as_ref().unwrap()).is_some());
    }

    #[test]
    fn logical_planner_plans_clickbench_queries() {
        let queries_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmark/clickbench/queries");
        let mut query_paths = std::fs::read_dir(queries_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "sql"))
            .collect::<Vec<_>>();
        query_paths.sort();

        for query_path in query_paths {
            let sql = std::fs::read_to_string(&query_path).unwrap();
            let ast = Parser::parse_sql(&PostgreSqlDialect {}, &sql)
                .unwrap_or_else(|error| panic!("failed to parse {}: {error}", query_path.display()))
                .remove(0);
            plan_query_statement(
                ast,
                vec![make_clickbench_hits_table()],
                &function_registry(),
            )
            .unwrap_or_else(|error| panic!("failed to plan {}: {error}", query_path.display()));
        }
    }

    #[test]
    fn standalone_fragment_planner_keeps_aggregate_in_single_fragment() {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "select count(id) from orders")
            .unwrap()
            .remove(0);
        let logical_plan =
            plan_query_statement(ast, vec![make_table("orders")], &function_registry()).unwrap();

        let plan = crate::planner::StandaloneFragmentPlanner::default()
            .plan_fragments(
                QueryContext::for_test(uuid::Uuid::new_v4()),
                logical_plan,
                crate::storage::open_storage_engine().unwrap(),
            )
            .unwrap();

        assert_eq!(plan.fragments.len(), 1);
        assert!(plan.exchanges.is_empty());
        assert_eq!(plan.table_scan_splits.len(), 1);
        assert!(matches!(
            plan.fragments[0].root,
            Some(DataFusionLogicalPlan::Aggregate(_)) | Some(DataFusionLogicalPlan::Projection(_))
        ));
    }

    #[test]
    fn logical_planner_keeps_aliased_aggregate_out_of_projection() {
        let plan = build_query_plan(
            "select count(id) as lineitem_count from orders",
            vec![make_table("orders")],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");
        assert_eq!(aggregate.aggr_expr.len(), 1);
        assert!(matches!(
            aggregate.aggr_expr[0],
            DataFusionExpr::Alias(_) | DataFusionExpr::AggregateFunction(_)
        ));
    }

    #[test]
    fn logical_planner_rewrites_count_star_to_count_one() {
        let plan = build_query_plan(
            "select count(*) as lineitem_count from orders",
            vec![make_table("orders")],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");
        assert_eq!(aggregate.aggr_expr.len(), 1);
        let DataFusionExpr::AggregateFunction(function) = &aggregate.aggr_expr[0] else {
            panic!("expected aggregate function");
        };
        assert!(matches!(
            function.params.args.as_slice(),
            [DataFusionExpr::Literal(_, _)]
        ));
    }

    #[test]
    fn logical_planner_rewrites_count_without_args_to_count_one() {
        let plan = build_query_plan("select count() from orders", vec![make_table("orders")]);
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");
        assert_eq!(aggregate.aggr_expr.len(), 1);
        let DataFusionExpr::AggregateFunction(function) = &aggregate.aggr_expr[0] else {
            panic!("expected aggregate function");
        };
        assert!(matches!(
            function.params.args.as_slice(),
            [DataFusionExpr::Literal(_, _)]
        ));
    }

    #[test]
    fn logical_planner_builds_distinct_order_limit_tree() {
        let plan = build_query_plan(
            "select distinct id from orders order by id desc limit 5",
            vec![make_table("orders")],
        );

        let DataFusionLogicalPlan::Sort(sort) =
            plan.fragments[0].root.as_ref().expect("expected root plan")
        else {
            panic!("expected sort root");
        };
        assert_eq!(sort.fetch, Some(5));
        assert_eq!(sort.expr.len(), 1);
        assert!(!sort.expr[0].asc);
        let DataFusionLogicalPlan::Aggregate(aggregate) = sort.input.as_ref() else {
            panic!("expected distinct aggregate under sort");
        };
        assert_eq!(aggregate.group_expr.len(), 1);
        assert!(aggregate.aggr_expr.is_empty());
    }

    #[test]
    fn logical_planner_builds_distinct_on_plan() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select distinct on (name) id, name from orders order by name, id desc",
        )
        .unwrap()
        .remove(0);
        let plan = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect("expected distinct on plan");

        let distinct = find_distinct(&plan).expect("expected distinct node");
        let datafusion_expr::logical_plan::Distinct::On(distinct_on) = distinct else {
            panic!("expected distinct on node");
        };
        assert_eq!(distinct_on.on_expr.len(), 1);
        assert_eq!(distinct_on.select_expr.len(), 2);
    }

    #[test]
    fn logical_planner_builds_values_query_plan() {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "values (1, 'a'), (2, 'b')")
            .unwrap()
            .remove(0);
        let plan = plan_query_statement(ast, Vec::new(), &function_registry())
            .expect("expected values query plan");

        let DataFusionLogicalPlan::Values(values) = plan else {
            panic!("expected values plan");
        };
        assert_eq!(values.values.len(), 2);
        assert_eq!(values.values[0].len(), 2);
    }

    #[test]
    fn logical_planner_builds_non_recursive_cte_plan() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "with c as (select id from orders) select id from c",
        )
        .unwrap()
        .remove(0);
        let plan = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect("expected cte query plan");

        find_projection(&plan).expect("expected projection plan");
        let scan = find_table_scan(&plan).expect("expected table scan in cte body");
        assert_eq!(scan.table_name.table(), "orders");
    }

    #[test]
    fn logical_planner_resolves_cte_before_same_named_table() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "with orders as (select UserID as id from hits) select id from orders",
        )
        .unwrap()
        .remove(0);
        let plan = plan_query_statement(
            ast,
            vec![make_table("orders"), make_hits_table()],
            &function_registry(),
        )
        .expect("expected cte to shadow same named table");

        let scan = find_table_scan(&plan).expect("expected table scan in cte body");
        assert_eq!(scan.table_name.table(), "hits");
    }

    #[test]
    fn logical_planner_allows_cte_reference_inside_derived_table() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "with c as (select id from orders) select id from (select id from c) d",
        )
        .unwrap()
        .remove(0);
        let plan = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect("expected derived table to see cte");

        let scan = find_table_scan(&plan).expect("expected table scan in cte body");
        assert_eq!(scan.table_name.table(), "orders");
    }

    #[test]
    fn logical_planner_builds_union_all_plan() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select id from orders union all select id from orders",
        )
        .unwrap()
        .remove(0);
        let plan = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect("expected union all plan");

        let DataFusionLogicalPlan::Union(union) = plan else {
            panic!("expected union plan");
        };
        assert_eq!(union.inputs.len(), 2);
    }

    #[test]
    fn logical_planner_applies_order_by_and_limit_to_set_operation() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select id from orders union all select id from orders order by id limit 1",
        )
        .unwrap()
        .remove(0);
        let plan = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect("expected ordered union plan");

        let DataFusionLogicalPlan::Limit(limit) = plan else {
            panic!("expected limit root");
        };
        assert!(limit.fetch.is_some());
        assert!(matches!(
            limit.input.as_ref(),
            DataFusionLogicalPlan::Sort(_)
        ));
    }

    #[test]
    fn logical_planner_rejects_distinct_on_with_group_by() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select distinct on (name) name, count(*) from orders group by name",
        )
        .unwrap()
        .remove(0);
        let err = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect_err("expected distinct on with group by to be rejected");

        assert!(err.to_string().contains("DISTINCT ON expressions"));
    }

    #[test]
    fn logical_planner_uses_datafusion_default_null_ordering() {
        let plan = build_query_plan(
            "select id from orders order by id desc",
            vec![make_table("orders")],
        );
        let sort = find_sort(plan.fragments[0].root.as_ref().unwrap()).expect("expected sort");

        assert_eq!(sort.expr.len(), 1);
        assert!(!sort.expr[0].asc);
        assert!(sort.expr[0].nulls_first);
    }

    #[test]
    fn logical_planner_resolves_group_by_position() {
        let plan = build_query_plan(
            "select id, count(*) from orders group by 1",
            vec![make_table("orders")],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");

        assert_eq!(aggregate.group_expr.len(), 1);
        assert!(matches!(aggregate.group_expr[0], DataFusionExpr::Column(_)));
    }

    #[test]
    fn logical_planner_expands_group_by_all_to_non_aggregate_projection() {
        let plan = build_query_plan(
            "select id, name, count(*) from orders group by all",
            vec![make_table("orders")],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");
        let group_names = aggregate
            .group_expr
            .iter()
            .map(|expr| match expr {
                DataFusionExpr::Column(column) => column.name.as_str(),
                _ => panic!("expected group by column"),
            })
            .collect::<Vec<_>>();

        assert_eq!(group_names, ["id", "name"]);
    }

    #[test]
    fn logical_planner_collects_having_aggregate() {
        let plan = build_query_plan(
            "select id from orders group by id having count(*) > 1",
            vec![make_table("orders")],
        );
        find_filter(plan.fragments[0].root.as_ref().unwrap()).expect("expected having filter");
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");

        assert_eq!(aggregate.aggr_expr.len(), 1);
    }

    #[test]
    fn logical_planner_rejects_having_without_group_by_or_aggregate() {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "select id from orders having id > 1")
            .unwrap()
            .remove(0);
        let err = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect_err("expected invalid having plan");

        assert!(err.to_string().contains("HAVING clause references"));
    }

    #[test]
    fn logical_planner_resolves_group_by_alias() {
        let plan = build_query_plan(
            "select id as order_id, count(*) from orders group by order_id",
            vec![make_table("orders")],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");

        assert_eq!(aggregate.group_expr.len(), 1);
        assert!(matches!(aggregate.group_expr[0], DataFusionExpr::Column(_)));
    }

    #[test]
    fn logical_planner_resolves_group_by_alias_case_insensitively() {
        let plan = build_query_plan(
            "select id as OrderID, count(*) from orders group by orderid",
            vec![make_table("orders")],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");

        assert_eq!(aggregate.group_expr.len(), 1);
        assert!(matches!(aggregate.group_expr[0], DataFusionExpr::Column(_)));
    }

    #[test]
    fn logical_planner_prefers_input_column_over_conflicting_group_by_alias() {
        let plan = build_query_plan(
            "select id as name, count(*) from orders group by name, id",
            vec![make_table("orders")],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");
        let group_names = aggregate
            .group_expr
            .iter()
            .map(|expr| match expr {
                DataFusionExpr::Column(column) => column.name.as_str(),
                _ => panic!("expected group by column"),
            })
            .collect::<Vec<_>>();

        assert_eq!(group_names, ["name", "id"]);
    }

    #[test]
    fn logical_planner_resolves_having_alias() {
        let plan = build_query_plan(
            "select id, count(*) as order_count from orders group by id having order_count > 1",
            vec![make_table("orders")],
        );
        let filter =
            find_filter(plan.fragments[0].root.as_ref().unwrap()).expect("expected having filter");
        let DataFusionExpr::BinaryExpr(binary) = &filter.predicate else {
            panic!("expected binary having predicate");
        };

        assert!(matches!(binary.left.as_ref(), DataFusionExpr::Column(_)));
    }

    #[test]
    fn logical_planner_resolves_order_by_position() {
        let plan = build_query_plan(
            "select id from orders order by 1 desc",
            vec![make_table("orders")],
        );
        let DataFusionLogicalPlan::Sort(sort) =
            plan.fragments[0].root.as_ref().expect("expected root plan")
        else {
            panic!("expected sort root");
        };

        assert_eq!(sort.expr.len(), 1);
        assert!(!sort.expr[0].asc);
        assert!(matches!(sort.expr[0].expr, DataFusionExpr::Column(_)));
    }

    #[test]
    fn logical_planner_allows_order_by_unprojected_input_column() {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "select id from orders order by name")
            .unwrap()
            .remove(0);
        let plan = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect("expected order by input column to plan");
        let sort = find_sort(&plan).expect("expected sort");

        assert_eq!(sort.expr.len(), 1);
        let DataFusionExpr::Column(column) = &sort.expr[0].expr else {
            panic!("expected sort column");
        };
        assert_eq!(column.name, "name");
    }

    #[test]
    fn logical_planner_resolves_order_by_alias_case_insensitively() {
        let plan = build_query_plan(
            "select count(*) as PageViews from hits group by url order by pageviews desc",
            vec![make_hits_table()],
        );
        let DataFusionLogicalPlan::Sort(sort) =
            plan.fragments[0].root.as_ref().expect("expected root plan")
        else {
            panic!("expected sort root");
        };

        assert_eq!(sort.expr.len(), 1);
        assert!(!sort.expr[0].asc);
    }

    #[test]
    fn logical_planner_collects_order_by_aggregate_not_in_projection() {
        let plan = build_query_plan(
            "select url from hits group by url order by count(*) desc",
            vec![make_hits_table()],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");

        assert_eq!(aggregate.aggr_expr.len(), 1);
        assert!(find_sort(plan.fragments[0].root.as_ref().unwrap()).is_some());
    }

    #[test]
    fn logical_planner_binds_aggregate_function_order_by_clause() {
        let plan = build_query_plan(
            "select array_agg(name order by id desc) from orders",
            vec![make_table("orders")],
        );
        let aggregate = find_aggregate(plan.fragments[0].root.as_ref().unwrap())
            .expect("expected aggregate plan");
        let DataFusionExpr::AggregateFunction(function) = &aggregate.aggr_expr[0] else {
            panic!("expected aggregate function");
        };
        assert_eq!(function.params.order_by.len(), 1);
        assert!(!function.params.order_by[0].asc);
    }

    #[test]
    fn logical_planner_allows_order_by_unprojected_group_key() {
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "select count(*) from orders group by name order by name",
        )
        .unwrap()
        .remove(0);
        let plan = plan_query_statement(ast, vec![make_table("orders")], &function_registry())
            .expect("expected order by group key to plan");
        let sort = find_sort(&plan).expect("expected sort");

        assert_eq!(sort.expr.len(), 1);
        let DataFusionExpr::Column(column) = &sort.expr[0].expr else {
            panic!("expected sort column");
        };
        assert_eq!(column.name, "name");
    }

    #[test]
    fn logical_planner_resolves_order_by_position_to_projected_alias() {
        let plan = build_query_plan(
            "select id as order_id from orders order by 1 desc",
            vec![make_table("orders")],
        );
        let sort = find_sort(plan.fragments[0].root.as_ref().unwrap()).expect("expected sort");
        let DataFusionExpr::Column(column) = &sort.expr[0].expr else {
            panic!("expected order by position to resolve to projected column");
        };

        assert_eq!(column.name, "order_id");
    }

    #[test]
    fn distributed_fragment_planner_builds_join_tree() {
        let plan = build_query_plan(
            "select * from orders o join customers c on o.id = c.id and o.id > 10",
            vec![make_table("orders"), make_table("customers")],
        );
        assert_eq!(plan.fragments.len(), 3);
        assert_eq!(plan.exchanges.len(), 2);
        eprintln!("non_column plan = {:#?}", plan.exchanges);
        eprintln!("non_column exchanges = {:#?}", plan.exchanges);
        assert!(plan
            .exchanges
            .iter()
            .all(|edge| edge.scope == ExchangeScope::Remote));
        assert!(plan
            .exchanges
            .iter()
            .all(|edge| edge.exchange_type == ExchangeType::Repartition));
        assert!(plan
            .exchanges
            .iter()
            .all(|edge| edge.partitioning_scheme.partition_keys.len() == 1));
        assert!(plan
            .exchanges
            .iter()
            .all(|edge| !edge.partitioning_scheme.output_layout.is_empty()));
        assert!(plan.fragments[1..]
            .iter()
            .all(|fragment| fragment.kind == PlanFragmentKind::Source));
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
        assert!(left_exchange
            .node
            .as_any()
            .downcast_ref::<RemoteSourceNode>()
            .is_some());
        let DataFusionLogicalPlan::Extension(right_exchange) = join.right.as_ref() else {
            panic!("expected right exchange placeholder");
        };
        assert!(right_exchange
            .node
            .as_any()
            .downcast_ref::<RemoteSourceNode>()
            .is_some());
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
    fn distributed_fragment_planner_keeps_non_column_equality_in_join_filter() {
        let plan = build_query_plan(
            "select * from orders o join customers c on lower(o.name) = lower(c.name)",
            vec![make_table("orders"), make_table("customers")],
        );
        assert_eq!(plan.fragments.len(), 3);
        assert_eq!(plan.exchanges.len(), 2);
        assert!(plan
            .exchanges
            .iter()
            .all(|edge| edge.scope == ExchangeScope::Remote));
        assert!(plan
            .exchanges
            .iter()
            .all(|edge| !edge.partitioning_scheme.output_layout.is_empty()));
        let Some(DataFusionLogicalPlan::Join(join)) = &plan.fragments[0].root else {
            panic!("expected join input");
        };
        assert!(join.on.len() <= 1);
    }

    #[test]
    fn logical_planner_builds_join_using_plan() {
        let plan = build_query_plan(
            "select * from orders join customers using (id)",
            vec![make_table("orders"), make_table("customers")],
        );
        let Some(DataFusionLogicalPlan::Join(join)) = &plan.fragments[0].root else {
            panic!("expected join input");
        };
        assert_eq!(join.join_type, DataFusionJoinType::Inner);
        assert_eq!(join.on.len(), 1);
    }

    #[test]
    fn logical_planner_builds_natural_join_plan() {
        let plan = build_query_plan(
            "select * from orders natural join customers",
            vec![make_table("orders"), make_table("customers")],
        );
        let Some(DataFusionLogicalPlan::Join(join)) = &plan.fragments[0].root else {
            panic!("expected join input");
        };
        assert_eq!(join.join_type, DataFusionJoinType::Inner);
        assert!(!join.on.is_empty());
    }
}
