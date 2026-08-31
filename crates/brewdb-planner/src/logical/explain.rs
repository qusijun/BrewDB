use std::sync::Arc;

use crate::parser::ast::{AnalyzeFormatKind, Statement as AstStatement};
use crate::planner::PlannerError;
use datafusion_common::display::{PlanType, ToStringifiedPlan};
use datafusion_common::DFSchema;
use datafusion_expr::logical_plan::ExplainFormat;
use datafusion_expr::{Analyze, Explain, ExplainOption, LogicalPlan as DataFusionLogicalPlan};
use std::str::FromStr;

use crate::planner::logical::{LogicalPlanner, LogicalPlanningContext};

pub(crate) fn bind_explain_statement(
    planner: &LogicalPlanner,
    statement: &AstStatement,
    analyze: bool,
    verbose: bool,
    format: Option<AnalyzeFormatKind>,
    ctx: &LogicalPlanningContext<'_>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let input = planner.plan(statement.clone(), ctx)?;
    let explain_option = explain_option(verbose, analyze, format)?;
    let output_schema = Arc::new(
        DFSchema::try_from(DataFusionLogicalPlan::explain_schema()).map_err(|error| {
            PlannerError::InvalidPlan {
                reason: error.to_string(),
            }
        })?,
    );

    if explain_option.analyze {
        return Ok(DataFusionLogicalPlan::Analyze(Analyze {
            verbose: explain_option.verbose,
            input: Arc::new(input),
            schema: output_schema,
        }));
    }

    let stringified_plans = vec![input.to_stringified(PlanType::InitialLogicalPlan)];
    Ok(DataFusionLogicalPlan::Explain(Explain {
        verbose: explain_option.verbose,
        explain_format: explain_option.format,
        plan: Arc::new(input),
        stringified_plans,
        schema: output_schema,
        logical_optimization_succeeded: false,
    }))
}

fn explain_option(
    verbose: bool,
    analyze: bool,
    format: Option<AnalyzeFormatKind>,
) -> Result<ExplainOption, PlannerError> {
    if verbose && format.is_some() {
        return Err(PlannerError::UnsupportedPlan {
            reason: "EXPLAIN VERBOSE with FORMAT is not supported".to_string(),
        });
    }

    if analyze && format.is_some() {
        return Err(PlannerError::UnsupportedPlan {
            reason: "EXPLAIN ANALYZE with FORMAT is not supported".to_string(),
        });
    }

    let mut option = ExplainOption::default()
        .with_verbose(verbose)
        .with_analyze(analyze);
    if let Some(format) = format {
        option = option.with_format(explain_option_format(format)?);
    }
    Ok(option)
}

fn explain_option_format(format: AnalyzeFormatKind) -> Result<ExplainFormat, PlannerError> {
    let format = match format {
        AnalyzeFormatKind::Keyword(format) | AnalyzeFormatKind::Assignment(format) => {
            format.to_string()
        }
    };
    ExplainFormat::from_str(&format).map_err(PlannerError::from)
}
