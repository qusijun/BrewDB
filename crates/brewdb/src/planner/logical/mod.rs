//! SQL statement logical planning.

pub mod command;
pub(crate) mod ddl;
pub(crate) mod explain;
mod expr;
pub(crate) mod mutation;
pub mod optimizer;
pub mod plan;
mod planner;
pub(crate) mod query;
pub(crate) mod session;
pub(crate) mod table_source;
pub(crate) mod transaction;

pub use optimizer::LogicalOptimizer;
pub use planner::{LogicalPlanner, LogicalPlanningContext, LogicalPlanningSession};

pub(crate) use planner::{
    empty_df_schema, extension_plan, name_parts, object_name_to_string, planner_to_sql_error,
    qualify_database_name, qualify_table_name, resolve_query_tables, resolve_table,
    resolve_table_object,
};
