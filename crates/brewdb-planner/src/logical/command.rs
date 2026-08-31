use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use datafusion_expr::{DdlStatement, Statement as DataFusionStatement};

use crate::planner::logical::plan::LogicalPlanNode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandPlan {
    Ddl(DdlStatement),
    Extension(LogicalPlanNode),
    Statement(DataFusionStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandTag {
    Select,
    Explain,
    ExplainAnalyze,
    Insert,
    CreateTable,
    DropTable,
    Ddl,
    ShowCatalogs,
    ShowDatabases,
    ShowTables,
    CreateDatabase,
    DropDatabase,
    Statement,
    Extension,
}

impl CommandTag {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Explain => "EXPLAIN",
            Self::ExplainAnalyze => "EXPLAIN ANALYZE",
            Self::Insert => "INSERT",
            Self::CreateTable => "CREATE TABLE",
            Self::DropTable => "DROP TABLE",
            Self::Ddl => "DDL",
            Self::ShowCatalogs => "SHOW CATALOGS",
            Self::ShowDatabases => "SHOW DATABASES",
            Self::ShowTables => "SHOW TABLES",
            Self::CreateDatabase => "CREATE DATABASE",
            Self::DropDatabase => "DROP DATABASE",
            Self::Statement => "STATEMENT",
            Self::Extension => "EXTENSION",
        }
    }
}

pub fn command_plan(root: &DataFusionLogicalPlan) -> Option<CommandPlan> {
    match root {
        DataFusionLogicalPlan::Ddl(statement) => Some(CommandPlan::Ddl(statement.clone())),
        DataFusionLogicalPlan::Extension(extension) => extension
            .node
            .as_any()
            .downcast_ref::<crate::planner::logical::plan::LogicalPlanNode>()
            .cloned()
            .map(CommandPlan::Extension),
        DataFusionLogicalPlan::Statement(statement) => {
            Some(CommandPlan::Statement(statement.clone()))
        }
        _ => None,
    }
}

pub(crate) fn command_tag(root: &DataFusionLogicalPlan) -> CommandTag {
    match root {
        DataFusionLogicalPlan::Analyze(_) => CommandTag::ExplainAnalyze,
        DataFusionLogicalPlan::Explain(_) => CommandTag::Explain,
        DataFusionLogicalPlan::Dml(_) => CommandTag::Insert,
        DataFusionLogicalPlan::Ddl(statement) => match statement {
            datafusion_expr::DdlStatement::CreateExternalTable(_) => CommandTag::CreateTable,
            datafusion_expr::DdlStatement::DropTable(_) => CommandTag::DropTable,
            _ => CommandTag::Ddl,
        },
        DataFusionLogicalPlan::Extension(extension) => extension
            .node
            .as_any()
            .downcast_ref::<crate::planner::logical::plan::LogicalPlanNode>()
            .map(command_tag_for_extension)
            .unwrap_or(CommandTag::Extension),
        DataFusionLogicalPlan::Statement(_) => CommandTag::Statement,
        _ => CommandTag::Select,
    }
}

pub(crate) fn returns_rows(root: &DataFusionLogicalPlan) -> bool {
    match root {
        DataFusionLogicalPlan::Dml(_) | DataFusionLogicalPlan::Ddl(_) => false,
        DataFusionLogicalPlan::Extension(extension) => extension
            .node
            .as_any()
            .downcast_ref::<crate::planner::logical::plan::LogicalPlanNode>()
            .is_some_and(returns_rows_for_extension),
        DataFusionLogicalPlan::Statement(_) => false,
        _ => true,
    }
}

fn command_tag_for_extension(node: &crate::planner::logical::plan::LogicalPlanNode) -> CommandTag {
    match node {
        crate::planner::logical::plan::LogicalPlanNode::Show(
            crate::planner::logical::plan::Show::Catalogs,
        ) => CommandTag::ShowCatalogs,
        crate::planner::logical::plan::LogicalPlanNode::Show(
            crate::planner::logical::plan::Show::Databases { .. },
        ) => CommandTag::ShowDatabases,
        crate::planner::logical::plan::LogicalPlanNode::Show(
            crate::planner::logical::plan::Show::Tables { .. },
        ) => CommandTag::ShowTables,
        crate::planner::logical::plan::LogicalPlanNode::Ddl(
            crate::planner::logical::plan::Ddl::CreateDatabase(_),
        ) => CommandTag::CreateDatabase,
        crate::planner::logical::plan::LogicalPlanNode::Ddl(
            crate::planner::logical::plan::Ddl::DropDatabase(_),
        ) => CommandTag::DropDatabase,
    }
}

fn returns_rows_for_extension(node: &crate::planner::logical::plan::LogicalPlanNode) -> bool {
    matches!(
        node,
        crate::planner::logical::plan::LogicalPlanNode::Show(_)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::DFSchema;
    use datafusion_expr::{Analyze, EmptyRelation, LogicalPlan};

    use super::{command_tag, CommandTag};

    #[test]
    fn command_tag_distinguishes_explain_analyze() {
        let plan = LogicalPlan::Analyze(Analyze {
            verbose: false,
            input: Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            })),
            schema: Arc::new(DFSchema::empty()),
        });

        assert_eq!(command_tag(&plan), CommandTag::ExplainAnalyze);
        assert_eq!(command_tag(&plan).as_str(), "EXPLAIN ANALYZE");
    }
}
