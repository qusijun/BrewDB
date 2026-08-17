use crate::parser::ast::{TransactionAccessMode, TransactionMode as AstTransactionMode};
use crate::SqlError;
use datafusion_expr::{
    LogicalPlan as DataFusionLogicalPlan, Statement as DataFusionStatement,
    TransactionAccessMode as DataFusionTransactionAccessMode,
    TransactionConclusion as DataFusionTransactionConclusion,
    TransactionEnd as DataFusionTransactionEnd, TransactionIsolationLevel, TransactionStart,
};

pub(crate) fn bind_start_transaction_statement(
    modes: &[AstTransactionMode],
) -> Result<DataFusionLogicalPlan, SqlError> {
    Ok(DataFusionLogicalPlan::Statement(
        DataFusionStatement::TransactionStart(TransactionStart {
            access_mode: bind_txn_access_mode(modes),
            isolation_level: TransactionIsolationLevel::ReadCommitted,
        }),
    ))
}

pub(crate) fn bind_commit_statement() -> Result<DataFusionLogicalPlan, SqlError> {
    Ok(DataFusionLogicalPlan::Statement(
        DataFusionStatement::TransactionEnd(DataFusionTransactionEnd {
            conclusion: DataFusionTransactionConclusion::Commit,
            chain: false,
        }),
    ))
}

pub(crate) fn bind_rollback_statement() -> Result<DataFusionLogicalPlan, SqlError> {
    Ok(DataFusionLogicalPlan::Statement(
        DataFusionStatement::TransactionEnd(DataFusionTransactionEnd {
            conclusion: DataFusionTransactionConclusion::Rollback,
            chain: false,
        }),
    ))
}

fn bind_txn_access_mode(modes: &[AstTransactionMode]) -> DataFusionTransactionAccessMode {
    if modes.iter().any(|mode| {
        matches!(
            mode,
            AstTransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
        )
    }) {
        DataFusionTransactionAccessMode::ReadOnly
    } else if modes.iter().any(|mode| {
        matches!(
            mode,
            AstTransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
        )
    }) {
        DataFusionTransactionAccessMode::ReadWrite
    } else {
        DataFusionTransactionAccessMode::ReadWrite
    }
}
