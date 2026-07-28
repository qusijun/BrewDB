//! Intent objects and entry shell emitted by the SQL frontend.

mod entry;

pub use entry::{
    CapabilityGate, DdlIntent, FrontendSqlRequest, FrontendSqlResult, InsertIntent, IntentPlanner,
    MaintenanceIntent, MutationIntent, QueryIntent, QueryOnlyIntentPlanner, SqlIntent,
    StatementClass,
};
