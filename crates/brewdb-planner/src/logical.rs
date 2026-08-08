//! Planner-owned logical plan shell.

use brewdb_catalog::TableCatalogEntry;
use datafusion_expr::{Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan};
use datafusion_sql::sqlparser::ast::Statement as AstStatement;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalPlan {
    Query(QueryPlanRoot),
    Insert(InsertPlanRoot),
    Delete(DeletePlanRoot),
    Update(UpdatePlanRoot),
    Merge(MergePlanRoot),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryPlanRoot {
    pub tables: Vec<TableCatalogEntry>,
    pub query: QueryExpression,
    pub input: Option<DataFusionLogicalPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryExpression {
    pub distinct: bool,
    pub projection: Vec<DataFusionExpr>,
    pub selection: Option<DataFusionExpr>,
    pub group_by: QueryGroupBy,
    pub having: Option<DataFusionExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryGroupBy {
    None,
    All,
    Expressions(Vec<DataFusionExpr>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertPlanRoot {
    pub target_table: TableCatalogEntry,
    pub source_tables: Vec<TableCatalogEntry>,
    pub ast: AstStatement,
    pub input: Option<DataFusionLogicalPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeletePlanRoot {
    pub target_table: TableCatalogEntry,
    pub ast: AstStatement,
    pub input: Option<DataFusionLogicalPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdatePlanRoot {
    pub target_table: TableCatalogEntry,
    pub source_tables: Vec<TableCatalogEntry>,
    pub ast: AstStatement,
    pub input: Option<DataFusionLogicalPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergePlanRoot {
    pub target_table: TableCatalogEntry,
    pub source_tables: Vec<TableCatalogEntry>,
    pub ast: AstStatement,
    pub input: Option<DataFusionLogicalPlan>,
}
