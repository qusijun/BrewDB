//! SQL statement logical planner.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::catalog::{CatalogPath, CatalogService, TableCatalogEntry};
use crate::parser::ast::{ObjectName, ObjectNamePart, Statement as AstStatement, TableObject};

use crate::{SessionContext, SqlRequestContext, Statement};
use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::registry::MemoryFunctionRegistry;
use datafusion_expr::{Extension, LogicalPlan as DataFusionLogicalPlan};
use datafusion_functions as datafusion_scalar_functions;
use datafusion_functions_aggregate as datafusion_aggregate_functions;

use super::plan::LogicalPlanNode;
use crate::planner::errors::PlannerError;

use super::ddl::{
    bind_alter_statement, bind_create_database_statement, bind_create_table_statement,
    bind_drop_statement, bind_show_catalogs_statement, bind_show_databases_statement,
    bind_show_tables_statement, bind_show_variable_statement,
};
use super::explain::bind_explain_statement;
use super::mutation::{
    bind_copy_statement, bind_delete_statement, bind_insert_statement, bind_merge_statement,
    bind_update_statement,
};
use super::query::bind_query_statement;
use super::session::{bind_set_statement, bind_use_statement};
use super::transaction::{
    bind_commit_statement, bind_rollback_statement, bind_start_transaction_statement,
};

pub struct LogicalPlanningContext<'a> {
    pub session: &'a SessionContext,
    pub request: &'a SqlRequestContext,
    pub catalog_service: &'a CatalogService,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalPlanningSession {
    pub session_id: uuid::Uuid,
    pub user_name: String,
    pub catalog_name: String,
    pub database_name: String,
}

#[derive(Debug)]
pub struct LogicalPlanner {
    function_registry: MemoryFunctionRegistry,
}

impl Default for LogicalPlanner {
    fn default() -> Self {
        Self {
            function_registry: default_function_registry(),
        }
    }
}

impl LogicalPlanner {
    pub fn plan(
        &self,
        statement: Statement,
        ctx: &LogicalPlanningContext<'_>,
    ) -> Result<DataFusionLogicalPlan, PlannerError> {
        let planning_session = planning_session(ctx)?;
        let ast = statement.clone();
        match &ast {
            AstStatement::Query(query) => bind_query_statement(
                statement,
                &planning_session,
                ctx,
                query,
                &self.function_registry,
            ),
            AstStatement::Insert(insert) => bind_insert_statement(
                statement,
                &planning_session,
                ctx,
                insert,
                &self.function_registry,
            ),
            AstStatement::Copy {
                source,
                to,
                target,
                options,
                legacy_options,
                values,
            } => bind_copy_statement(
                &planning_session,
                ctx,
                source,
                *to,
                target,
                options,
                legacy_options,
                values,
            ),
            AstStatement::Delete(delete) => {
                bind_delete_statement(statement, &planning_session, ctx, delete)
            }
            AstStatement::Update(update) => {
                bind_update_statement(statement, &planning_session, ctx, update)
            }
            AstStatement::Merge(merge) => {
                bind_merge_statement(statement, &planning_session, ctx, merge)
            }
            AstStatement::CreateDatabase { db_name, .. } => {
                bind_create_database_statement(planning_session, db_name)
            }
            AstStatement::CreateTable(create_table) => {
                bind_create_table_statement(ctx, &planning_session, create_table)
            }
            AstStatement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => bind_drop_statement(&planning_session, ctx, object_type, *if_exists, names),
            AstStatement::AlterTable(alter_table) => {
                bind_alter_statement(planning_session, ctx, alter_table)
            }
            AstStatement::ShowCatalogs { .. } => bind_show_catalogs_statement(),
            AstStatement::ShowDatabases { show_options, .. }
            | AstStatement::ShowSchemas { show_options, .. } => {
                bind_show_databases_statement(planning_session, show_options)
            }
            AstStatement::ShowTables { show_options, .. } => {
                bind_show_tables_statement(planning_session, show_options)
            }
            AstStatement::ShowVariable { variable } => bind_show_variable_statement(variable),
            AstStatement::Set(set) => bind_set_statement(set, planning_session),
            AstStatement::Use(use_stmt) => bind_use_statement(planning_session, use_stmt),
            AstStatement::StartTransaction { modes, .. } => bind_start_transaction_statement(modes),
            AstStatement::Commit { .. } => bind_commit_statement(),
            AstStatement::Rollback { .. } => bind_rollback_statement(),
            AstStatement::Explain {
                statement, format, ..
            } => bind_explain_statement(self, statement, *format, ctx),
            _ => Err(PlannerError::UnsupportedPlan {
                reason: statement.to_string(),
            }),
        }
    }
}

fn planning_session(
    ctx: &LogicalPlanningContext<'_>,
) -> Result<LogicalPlanningSession, PlannerError> {
    let catalog_name =
        ctx.session
            .catalog_name
            .clone()
            .ok_or_else(|| PlannerError::InvalidPlan {
                reason: "missing default catalog in session context".to_string(),
            })?;
    let database_name =
        ctx.session
            .database_name
            .clone()
            .ok_or_else(|| PlannerError::InvalidPlan {
                reason: "missing default database in session context".to_string(),
            })?;
    Ok(LogicalPlanningSession {
        session_id: ctx.session.session_id,
        user_name: ctx.session.user_name.clone(),
        catalog_name,
        database_name,
    })
}

fn default_function_registry() -> MemoryFunctionRegistry {
    let mut registry = MemoryFunctionRegistry::new();
    datafusion_scalar_functions::register_all(&mut registry)
        .expect("default DataFusion scalar functions must register");
    datafusion_aggregate_functions::register_all(&mut registry)
        .expect("default DataFusion aggregate functions must register");
    registry
}

pub(crate) fn empty_df_schema() -> DFSchemaRef {
    Arc::new(DFSchema::empty())
}

pub(crate) fn extension_plan(node: LogicalPlanNode) -> DataFusionLogicalPlan {
    DataFusionLogicalPlan::Extension(Extension {
        node: Arc::new(node),
    })
}

pub(crate) fn qualify_database_name(
    session: &LogicalPlanningSession,
    name: &ObjectName,
) -> Result<(String, String), PlannerError> {
    match name_parts(name)?.as_slice() {
        [database] => Ok((session.catalog_name.clone(), database.clone())),
        [catalog, database] => Ok((catalog.clone(), database.clone())),
        _ => Err(PlannerError::InvalidPlan {
            reason: format!("invalid database name `{name}`"),
        }),
    }
}

pub(crate) fn qualify_table_name(
    session: &LogicalPlanningSession,
    name: &ObjectName,
) -> Result<(String, String, String), PlannerError> {
    match name_parts(name)?.as_slice() {
        [table] => Ok((
            session.catalog_name.clone(),
            session.database_name.clone(),
            table.clone(),
        )),
        [database, table] => Ok((
            session.catalog_name.clone(),
            database.clone(),
            table.clone(),
        )),
        [catalog, database, table] => Ok((catalog.clone(), database.clone(), table.clone())),
        _ => Err(PlannerError::InvalidPlan {
            reason: format!("invalid table name `{name}`"),
        }),
    }
}

pub(crate) fn resolve_table(
    ctx: &LogicalPlanningContext<'_>,
    session: &LogicalPlanningSession,
    name: &ObjectName,
) -> Result<TableCatalogEntry, PlannerError> {
    let (catalog_name, database_name, table_name) = qualify_table_name(session, name)?;
    resolve_table_parts(ctx, &catalog_name, &database_name, &table_name)
}

pub(crate) fn resolve_table_object(
    ctx: &LogicalPlanningContext<'_>,
    session: &LogicalPlanningSession,
    table: &TableObject,
) -> Result<TableCatalogEntry, PlannerError> {
    match table {
        TableObject::TableName(name) => resolve_table(ctx, session, name),
        _ => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported table target `{table}`"),
        }),
    }
}

fn table_factor_object_name(factor: &crate::parser::ast::TableFactor) -> Option<&ObjectName> {
    match factor {
        crate::parser::ast::TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

pub(crate) fn resolve_query_tables(
    ctx: &LogicalPlanningContext<'_>,
    session: &LogicalPlanningSession,
    query: &crate::parser::ast::Query,
) -> Result<Vec<TableCatalogEntry>, PlannerError> {
    let mut names = Vec::new();
    collect_query_table_names(query, &mut names)?;
    let mut seen = BTreeSet::new();
    let mut tables = Vec::new();
    for name in names {
        let table = resolve_table(ctx, session, &name)?;
        if seen.insert(table.table_id) {
            tables.push(table);
        }
    }
    Ok(tables)
}

fn collect_query_table_names(
    query: &crate::parser::ast::Query,
    names: &mut Vec<ObjectName>,
) -> Result<(), PlannerError> {
    if query.with.is_some() {
        return Err(PlannerError::UnsupportedPlan {
            reason: "WITH queries are not supported yet".to_string(),
        });
    }
    collect_set_expr_table_names(query.body.as_ref(), names)
}

fn collect_set_expr_table_names(
    set_expr: &crate::parser::ast::SetExpr,
    names: &mut Vec<ObjectName>,
) -> Result<(), PlannerError> {
    match set_expr {
        crate::parser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                collect_table_factor_names(&from.relation, names)?;
                for join in &from.joins {
                    collect_table_factor_names(&join.relation, names)?;
                }
            }
            for projection in &select.projection {
                collect_select_item_table_names(projection, names)?;
            }
            if let Some(selection) = &select.selection {
                collect_expr_table_names(selection, names)?;
            }
            match &select.group_by {
                crate::parser::ast::GroupByExpr::Expressions(expressions, _) => {
                    for expr in expressions {
                        collect_expr_table_names(expr, names)?;
                    }
                }
                crate::parser::ast::GroupByExpr::All(_) => {}
            }
            if let Some(having) = &select.having {
                collect_expr_table_names(having, names)?;
            }
            Ok(())
        }
        crate::parser::ast::SetExpr::Query(query) => collect_query_table_names(query, names),
        crate::parser::ast::SetExpr::Values(_) => Ok(()),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported query body `{other}`"),
        }),
    }
}

fn collect_select_item_table_names(
    item: &crate::parser::ast::SelectItem,
    names: &mut Vec<ObjectName>,
) -> Result<(), PlannerError> {
    match item {
        crate::parser::ast::SelectItem::UnnamedExpr(expr)
        | crate::parser::ast::SelectItem::ExprWithAlias { expr, .. }
        | crate::parser::ast::SelectItem::ExprWithAliases { expr, .. } => {
            collect_expr_table_names(expr, names)
        }
        crate::parser::ast::SelectItem::Wildcard(_)
        | crate::parser::ast::SelectItem::QualifiedWildcard(_, _) => Ok(()),
    }
}

fn collect_expr_table_names(
    expr: &crate::parser::ast::Expr,
    names: &mut Vec<ObjectName>,
) -> Result<(), PlannerError> {
    use crate::parser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

    match expr {
        Expr::Nested(expr)
        | Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => collect_expr_table_names(expr, names),
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_table_names(left, names)?;
            collect_expr_table_names(right, names)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_table_names(expr, names)?;
            collect_expr_table_names(low, names)?;
            collect_expr_table_names(high, names)
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_table_names(expr, names)?;
            for item in list {
                collect_expr_table_names(item, names)?;
            }
            Ok(())
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_expr_table_names(expr, names)?;
            collect_query_table_names(subquery, names)
        }
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
            collect_query_table_names(subquery, names)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            collect_expr_table_names(expr, names)?;
            collect_expr_table_names(pattern, names)
        }
        Expr::Interval(interval) => collect_expr_table_names(&interval.value, names),
        Expr::Extract { expr, .. } => collect_expr_table_names(expr, names),
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_expr_table_names(expr, names)?;
            if let Some(from) = substring_from {
                collect_expr_table_names(from, names)?;
            }
            if let Some(for_expr) = substring_for {
                collect_expr_table_names(for_expr, names)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_table_names(operand, names)?;
            }
            for condition in conditions {
                collect_expr_table_names(&condition.condition, names)?;
                collect_expr_table_names(&condition.result, names)?;
            }
            if let Some(else_result) = else_result {
                collect_expr_table_names(else_result, names)?;
            }
            Ok(())
        }
        Expr::Function(function) => {
            if let FunctionArguments::List(arguments) = &function.args {
                for arg in &arguments.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        } => collect_expr_table_names(expr, names)?,
                        _ => {}
                    }
                }
            }
            if let Some(filter) = &function.filter {
                collect_expr_table_names(filter, names)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_table_factor_names(
    factor: &crate::parser::ast::TableFactor,
    names: &mut Vec<ObjectName>,
) -> Result<(), PlannerError> {
    match factor {
        crate::parser::ast::TableFactor::Table { .. } => {
            let Some(name) = table_factor_object_name(factor) else {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported table factor `{factor}`"),
                });
            };
            names.push(name.clone());
            Ok(())
        }
        crate::parser::ast::TableFactor::Derived {
            lateral,
            subquery,
            sample,
            ..
        } => {
            if *lateral || sample.is_some() {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported table factor `{factor}`"),
                });
            }
            collect_query_table_names(subquery, names)
        }
        crate::parser::ast::TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            if alias.is_some() {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported nested join alias `{factor}`"),
                });
            }
            collect_table_factor_names(&table_with_joins.relation, names)?;
            for join in &table_with_joins.joins {
                collect_table_factor_names(&join.relation, names)?;
            }
            Ok(())
        }
        _ => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported table factor `{factor}`"),
        }),
    }
}

fn resolve_table_parts(
    ctx: &LogicalPlanningContext<'_>,
    catalog_name: &str,
    database_name: &str,
    table_name: &str,
) -> Result<TableCatalogEntry, PlannerError> {
    let path = CatalogPath::new(catalog_name).map_err(|error| PlannerError::InvalidPlan {
        reason: error.to_string(),
    })?;
    let catalog = ctx
        .catalog_service
        .open_catalog(path.catalog())
        .map_err(|error| PlannerError::InvalidPlan {
            reason: error.to_string(),
        })?;
    catalog
        .get_table(database_name, table_name)
        .map_err(|error| PlannerError::InvalidPlan {
            reason: error.to_string(),
        })
}

pub(crate) fn name_parts(name: &ObjectName) -> Result<Vec<String>, PlannerError> {
    name.0
        .iter()
        .map(object_name_part_value)
        .collect::<Result<Vec<_>, _>>()
}

fn object_name_part_value(part: &ObjectNamePart) -> Result<String, PlannerError> {
    match part {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
        ObjectNamePart::Function(_) => Err(PlannerError::UnsupportedPlan {
            reason: "dynamic object names are not supported".to_string(),
        }),
    }
}

pub(crate) fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::catalog::{
        open_catalog_store, CatalogConfig, CatalogEntry, CatalogMode, CatalogPath, CatalogService,
        CatalogStoreBackendKind, CreateDatabaseRequest, CreateTableRequest, StorageKind,
    };
    use crate::common::config::{global_config_registry, ConfigPatch, ConfigScope, ConfigSet};
    use crate::common::context::SessionContext;
    use crate::common::defaults::MANAGED_PAIMON_CATALOG_NAME;
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::frontend::ingress::SqlRequestContext;
    use crate::planner::PlannerError;
    use crate::runtime::driver::sql_to_statement;
    use datafusion_expr::TableSource;
    use uuid::Uuid;

    use crate::planner::logical::plan::{CreateDatabase, Ddl, DropDatabase, LogicalPlanNode, Show};
    use crate::planner::logical::table_source::DefaultTableSource;
    use crate::planner::logical::{LogicalOptimizer, LogicalPlanningContext};

    use super::LogicalPlanner;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).expect("test directory must be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn catalog_service() -> CatalogService {
        let store = open_catalog_store(&CatalogConfig {
            store_backend: CatalogStoreBackendKind::Memory,
            paimon_warehouse: String::new(),
        });
        let warehouse = TestDir::new("brewdb-logical-planner-tests");
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry("brewdb.catalog.store.backend", "memory")
                    .with_entry(
                        "brewdb.catalog.paimon.warehouse",
                        warehouse.path().to_string_lossy().as_ref(),
                    ),
            )
            .unwrap();
        let service = CatalogService::with_config(store, config);
        let entry = CatalogEntry::new(
            Uuid::new_v4(),
            CatalogPath::new(MANAGED_PAIMON_CATALOG_NAME).unwrap(),
            CatalogMode::Managed,
            StorageKind::Paimon,
        );
        service.create_catalog(entry).unwrap();
        let catalog = service.open_catalog(MANAGED_PAIMON_CATALOG_NAME).unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("brewdb"))
            .unwrap();
        catalog
            .create_table(
                CreateTableRequest::new(
                    "brewdb",
                    "orders",
                    TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();
        catalog
            .create_table(
                CreateTableRequest::new(
                    "brewdb",
                    "customers",
                    TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();
        catalog
            .create_table(
                CreateTableRequest::new(
                    "brewdb",
                    "tpch_exprs",
                    TableSchema::new(vec![
                        ColumnField::new("p_type", DataType::String),
                        ColumnField::new("p_size", DataType::Int32),
                        ColumnField::new("o_orderdate", DataType::Date),
                        ColumnField::new("l_shipdate", DataType::Date),
                    ]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();
        service
    }

    fn bind(sql: &str) -> datafusion_expr::LogicalPlan {
        bind_result(sql).unwrap()
    }

    fn bind_result(sql: &str) -> Result<datafusion_expr::LogicalPlan, PlannerError> {
        let service = catalog_service();
        let parsed = sql_to_statement(sql).unwrap();
        LogicalPlanner::default().plan(
            parsed,
            &LogicalPlanningContext {
                session: &SessionContext {
                    session_id: Uuid::nil(),
                    user_name: "brew".to_owned(),
                    catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                    database_name: Some("brewdb".to_owned()),
                    settings: ConfigSet::new(),
                },
                request: &SqlRequestContext {
                    request_id: Uuid::nil(),
                },
                catalog_service: &service,
            },
        )
    }

    fn plan_has_correlated_exists(plan: &datafusion_expr::LogicalPlan) -> bool {
        match plan {
            datafusion_expr::LogicalPlan::Filter(filter) => {
                expr_is_correlated_exists(&filter.predicate)
                    || plan_has_correlated_exists(&filter.input)
            }
            datafusion_expr::LogicalPlan::Projection(projection) => {
                plan_has_correlated_exists(&projection.input)
            }
            datafusion_expr::LogicalPlan::Aggregate(aggregate) => {
                plan_has_correlated_exists(&aggregate.input)
            }
            datafusion_expr::LogicalPlan::Sort(sort) => plan_has_correlated_exists(&sort.input),
            datafusion_expr::LogicalPlan::Limit(limit) => plan_has_correlated_exists(&limit.input),
            _ => false,
        }
    }

    fn expr_is_correlated_exists(expr: &datafusion_expr::Expr) -> bool {
        matches!(
            expr,
            datafusion_expr::Expr::Exists(exists)
                if !exists.subquery.outer_ref_columns.is_empty()
        )
    }

    #[test]
    fn logical_planner_turns_select_into_datafusion_logical_plan() {
        let planned = bind("select * from orders");

        match planned {
            datafusion_expr::LogicalPlan::Projection(_)
            | datafusion_expr::LogicalPlan::TableScan(_) => {}
            other => panic!("expected DataFusion logical plan, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_turns_insert_into_datafusion_logical_plan() {
        let planned = bind("insert into orders values (1), (2)");

        match planned {
            datafusion_expr::LogicalPlan::Dml(_) => {}
            other => panic!("expected DataFusion logical plan, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_binds_like_expression() {
        bind("select p_type from tpch_exprs where p_type like '%BRASS'");
    }

    #[test]
    fn logical_planner_binds_typed_date_expression() {
        bind("select o_orderdate from tpch_exprs where o_orderdate >= date '1995-03-15'");
    }

    #[test]
    fn logical_planner_binds_date_interval_expression() {
        bind(
            "select o_orderdate from tpch_exprs where o_orderdate < date '1995-03-15' + interval '3' month",
        );
    }

    #[test]
    fn logical_planner_binds_case_when_expression() {
        bind("select case when p_type like 'PROMO%' then 1 else 0 end as promo from tpch_exprs");
    }

    #[test]
    fn logical_planner_binds_in_list_expression() {
        bind("select p_size from tpch_exprs where p_size in (1, 2, 3)");
    }

    #[test]
    fn logical_planner_binds_extract_expression() {
        bind("select extract(year from l_shipdate) from tpch_exprs");
    }

    #[test]
    fn logical_planner_binds_substring_expression() {
        bind("select substring(p_type from 1 for 2) from tpch_exprs");
    }

    #[test]
    fn logical_planner_collects_multiple_nested_aggregates() {
        bind(
            "select sum(case when p_type like 'PROMO%' then p_size else 0 end) / sum(p_size) from tpch_exprs",
        );
    }

    #[test]
    fn logical_planner_binds_derived_table() {
        bind("select item_size from (select p_size as item_size from tpch_exprs) as items");
    }

    #[test]
    fn logical_planner_binds_derived_table_column_aliases() {
        bind("select renamed_size from (select p_size from tpch_exprs) as items (renamed_size)");
    }

    #[test]
    fn logical_planner_binds_scalar_subquery_expression() {
        bind("select p_size from tpch_exprs where p_size = (select max(p_size) from tpch_exprs)");
    }

    #[test]
    fn logical_planner_binds_in_subquery_expression() {
        bind("select p_size from tpch_exprs where p_size in (select p_size from tpch_exprs)");
    }

    #[test]
    fn logical_planner_binds_exists_subquery_expression() {
        bind("select p_size from tpch_exprs where exists (select p_size from tpch_exprs)");
    }

    #[test]
    fn logical_planner_binds_correlated_exists_subquery_expression() {
        let planned = bind(
            "select o.id from orders as o where exists (select * from customers as c where c.id = o.id)",
        );

        assert!(plan_has_correlated_exists(&planned));
    }

    #[test]
    fn logical_planner_honors_drop_table_if_exists_for_missing_table() {
        let planned = bind("drop table if exists missing_orders");

        assert!(matches!(
            planned,
            datafusion_expr::LogicalPlan::EmptyRelation(_)
        ));
    }

    #[test]
    fn logical_planner_rejects_drop_table_for_missing_table() {
        let error = bind_result("drop table missing_orders").unwrap_err();

        assert!(error.to_string().contains("table not found"));
    }

    #[test]
    fn logical_planner_turns_copy_from_csv_into_insert_plan() {
        let csv_path =
            std::env::temp_dir().join(format!("brewdb-copy-source-{}.csv", uuid::Uuid::new_v4()));
        std::fs::write(&csv_path, "id\n1\n").unwrap();
        let source_name =
            crate::common::utils::normalize_file_path(csv_path.to_string_lossy().as_ref());
        let planned = bind(&format!(
            "copy from '{}' to orders with (format csv, header true)",
            csv_path.display()
        ));

        match planned {
            datafusion_expr::LogicalPlan::Dml(dml) => {
                assert_eq!(
                    dml.table_name.to_string(),
                    "managed_paimon_catalog.brewdb.orders"
                );
                let datafusion_expr::LogicalPlan::Projection(projection) = dml.input.as_ref()
                else {
                    panic!("expected COPY FROM input to be a projection");
                };
                assert_eq!(projection.expr.len(), 1);
                let datafusion_expr::LogicalPlan::TableScan(scan) = projection.input.as_ref()
                else {
                    panic!("expected COPY FROM projection input to be a table scan");
                };
                assert_eq!(scan.table_name.table(), source_name);
                let source = scan
                    .source
                    .downcast_ref::<DefaultTableSource>()
                    .expect("COPY FROM source must be catalog-backed table source");
                let table = source.table();
                assert_eq!(table.storage_kind, StorageKind::File);
                assert_eq!(table.catalog_mode, CatalogMode::Temporary);
                assert_eq!(table.path.table(), source_name);
                assert_eq!(table.table_location, csv_path.to_string_lossy());
                assert_eq!(table.table_schema.fields.len(), 1);
                assert_eq!(table.table_schema.fields[0].name, "id");
                assert!(source.table_engine().is_some());
                assert_eq!(source.schema().field(0).name(), "id");
                assert_eq!(table.table_options.get("file_type"), None);
                assert_eq!(
                    table.table_options.get("format").map(String::as_str),
                    Some("csv")
                );
                assert_eq!(
                    table.table_options.get("has_header").map(String::as_str),
                    Some("true")
                );
            }
            other => panic!("expected DataFusion DML logical plan, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_turns_create_table_distribution_into_table_layout() {
        let planned = bind(
            "create table t1 (id int not null, name text) distributed by hash(id) into 8 buckets",
        );

        match planned {
            datafusion_expr::LogicalPlan::Ddl(
                datafusion_expr::DdlStatement::CreateExternalTable(statement),
            ) => {
                assert_eq!(
                    statement.name.to_string(),
                    "managed_paimon_catalog.brewdb.t1"
                );
                assert_eq!(statement.file_type, "paimon");
                assert_eq!(statement.schema.fields().len(), 2);
                assert!(!statement.schema.field(0).is_nullable());
                assert!(statement.options.is_empty());
                let table_schema = TableSchema::from_arrow_schema(statement.schema.as_arrow())
                    .expect("create table schema metadata must be readable");
                assert_eq!(table_schema.bucket_keys, vec!["id"]);
                assert_eq!(table_schema.bucket_function.as_deref(), Some("hash"));
                assert_eq!(table_schema.bucket_count, Some(8));
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_keeps_dotted_paimon_table_options() {
        let planned = bind("create table t1 (id int not null) with (file.format = vortex)");

        match planned {
            datafusion_expr::LogicalPlan::Ddl(
                datafusion_expr::DdlStatement::CreateExternalTable(statement),
            ) => {
                assert_eq!(
                    statement.options.get("file.format").map(String::as_str),
                    Some("vortex")
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_keeps_dotted_paimon_table_options_with_quoted_value() {
        let planned = bind("create table t1 (id int not null) with (file.format = 'vortex')");

        match planned {
            datafusion_expr::LogicalPlan::Ddl(
                datafusion_expr::DdlStatement::CreateExternalTable(statement),
            ) => {
                assert_eq!(
                    statement.options.get("file.format").map(String::as_str),
                    Some("vortex")
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_turns_create_table_partitioning_into_table_layout() {
        let planned = bind(
            "create table t1 (id int not null, dt text, region text) partitioned by (dt, region)",
        );

        match planned {
            datafusion_expr::LogicalPlan::Ddl(
                datafusion_expr::DdlStatement::CreateExternalTable(statement),
            ) => {
                assert!(statement.options.is_empty());
                assert_eq!(statement.table_partition_cols, vec!["dt", "region"]);
                let table_schema = TableSchema::from_arrow_schema(statement.schema.as_arrow())
                    .expect("create table schema metadata must be readable");
                assert_eq!(table_schema.partition_keys, vec!["dt", "region"]);
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_rejects_unknown_create_table_partition_column() {
        let service = catalog_service();
        let parsed = sql_to_statement("create table t1 (id int) partitioned by (dt)").unwrap();
        let error = LogicalPlanner::default()
            .plan(
                parsed,
                &LogicalPlanningContext {
                    session: &SessionContext {
                        session_id: Uuid::nil(),
                        user_name: "brew".to_owned(),
                        catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                        database_name: Some("brewdb".to_owned()),
                        settings: ConfigSet::new(),
                    },
                    request: &SqlRequestContext {
                        request_id: Uuid::nil(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid planner input: PARTITIONED BY partition column `dt` is not defined in CREATE TABLE"
        );
    }

    #[test]
    fn logical_planner_turns_cluster_by_into_table_layout() {
        let planned = bind("create table t1 (id int, dt text) cluster by (dt, id)");

        match planned {
            datafusion_expr::LogicalPlan::Ddl(
                datafusion_expr::DdlStatement::CreateExternalTable(statement),
            ) => {
                assert!(statement.options.is_empty());
                let table_schema = TableSchema::from_arrow_schema(statement.schema.as_arrow())
                    .expect("create table schema metadata must be readable");
                assert_eq!(table_schema.cluster_keys, vec!["dt", "id"]);
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_keeps_cluster_by_on_primary_key_as_generic_table_layout() {
        let planned = bind("create table t1 (id int primary key, dt text) cluster by (dt)");

        match planned {
            datafusion_expr::LogicalPlan::Ddl(
                datafusion_expr::DdlStatement::CreateExternalTable(statement),
            ) => {
                let table_schema = TableSchema::from_arrow_schema(statement.schema.as_arrow())
                    .expect("create table schema metadata must be readable");
                assert_eq!(table_schema.cluster_keys, vec!["dt"]);
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_rejects_unknown_create_table_cluster_column() {
        let service = catalog_service();
        let parsed = sql_to_statement("create table t1 (id int) cluster by (dt)").unwrap();
        let error = LogicalPlanner::default()
            .plan(
                parsed,
                &LogicalPlanningContext {
                    session: &SessionContext {
                        session_id: Uuid::nil(),
                        user_name: "brew".to_owned(),
                        catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                        database_name: Some("brewdb".to_owned()),
                        settings: ConfigSet::new(),
                    },
                    request: &SqlRequestContext {
                        request_id: Uuid::nil(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid planner input: CLUSTER BY cluster column `dt` is not defined in CREATE TABLE"
        );
    }

    #[test]
    fn logical_planner_turns_database_ddl_into_brewdb_extension_plans() {
        let planned = bind("create database wl_db");
        assert!(matches!(
            brewdb_logical_node(&planned),
            Some(LogicalPlanNode::Ddl(Ddl::CreateDatabase(CreateDatabase {
                catalog_name,
                database_name,
            }))) if catalog_name == MANAGED_PAIMON_CATALOG_NAME && database_name == "wl_db"
        ));

        let planned = bind("drop database brewdb");
        assert!(matches!(
            brewdb_logical_node(&planned),
            Some(LogicalPlanNode::Ddl(Ddl::DropDatabase(DropDatabase {
                catalog_name,
                database_name,
            }))) if catalog_name == MANAGED_PAIMON_CATALOG_NAME && database_name == "brewdb"
        ));
    }

    #[test]
    fn logical_planner_keeps_table_ddl_as_datafusion_ddl() {
        let planned = bind("drop table orders");
        assert!(matches!(
            planned,
            datafusion_expr::LogicalPlan::Ddl(datafusion_expr::DdlStatement::DropTable(_))
        ));
    }

    #[test]
    fn logical_planner_rejects_invalid_create_table_shapes() {
        let service = catalog_service();
        let parsed = sql_to_statement("create table t1").unwrap();
        let error = LogicalPlanner::default()
            .plan(
                parsed,
                &LogicalPlanningContext {
                    session: &SessionContext {
                        session_id: Uuid::nil(),
                        user_name: "brew".to_owned(),
                        catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                        database_name: Some("brewdb".to_owned()),
                        settings: ConfigSet::new(),
                    },
                    request: &SqlRequestContext {
                        request_id: Uuid::nil(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid planner input: CREATE TABLE must define at least one column"
        );
    }

    #[test]
    fn logical_planner_turns_show_and_set_into_logical_plans() {
        let planned = bind("show catalogs");
        assert!(matches!(brewdb_show_plan(&planned), Some(Show::Catalogs)));

        let planned = bind("show tables");
        match planned {
            datafusion_expr::LogicalPlan::Extension(_) => {
                let Some(Show::Tables {
                    catalog_name,
                    database_name,
                }) = brewdb_show_plan(&planned)
                else {
                    panic!("expected SHOW TABLES extension plan");
                };
                assert_eq!(catalog_name, MANAGED_PAIMON_CATALOG_NAME);
                assert_eq!(database_name, "brewdb");
            }
            other => panic!("expected show tables statement, got {other:?}"),
        }

        let planned = bind("set work_mem = '128MB'");
        assert!(matches!(
            planned,
            datafusion_expr::LogicalPlan::Statement(datafusion_expr::Statement::SetVariable(_))
        ));
    }

    #[test]
    fn logical_optimizer_applies_brewdb_extension_rules() {
        let planned = bind("show catalogs");
        let mut observed_rules = Vec::new();

        let optimized = LogicalOptimizer::default()
            .optimize_with_observer(planned, |_, rule| {
                observed_rules.push(rule.name().to_string());
            })
            .unwrap();

        assert!(matches!(brewdb_show_plan(&optimized), Some(Show::Catalogs)));
        assert!(observed_rules
            .iter()
            .any(|rule| rule == "brewdb_logical_extension"));
    }

    #[test]
    fn logical_optimizer_runs_datafusion_analyzer_before_optimization() {
        let planned = bind("select 1");
        let mut analyzer_rules = Vec::new();

        let _optimized = LogicalOptimizer::default()
            .optimize_with_observers(
                planned,
                |_, rule| analyzer_rules.push(rule.name().to_owned()),
                |_, _| {},
            )
            .unwrap();

        assert!(analyzer_rules
            .iter()
            .any(|rule| rule == "resolve_grouping_function"));
        assert!(analyzer_rules.iter().any(|rule| rule == "type_coercion"));
    }

    fn brewdb_show_plan(plan: &datafusion_expr::LogicalPlan) -> Option<&Show> {
        brewdb_logical_node(plan).and_then(LogicalPlanNode::show)
    }

    fn brewdb_logical_node(plan: &datafusion_expr::LogicalPlan) -> Option<&LogicalPlanNode> {
        let datafusion_expr::LogicalPlan::Extension(extension) = plan else {
            return None;
        };
        extension.node.as_any().downcast_ref::<LogicalPlanNode>()
    }
}
