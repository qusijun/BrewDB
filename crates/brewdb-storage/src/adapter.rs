//! Table-level adapter contracts.

use brewdb_core::catalog::FormatType;

use crate::append::AppendPlanner;
use crate::commit::CommitPreparationService;
use crate::rewrite::RewritePlanner;
use crate::scan::ScanPlanner;
use crate::statistics::StatisticsProvider;

/// Top-level storage adapter boundary for one table-format family.
pub trait StorageAdapter:
    ScanPlanner + AppendPlanner + RewritePlanner + CommitPreparationService + StatisticsProvider
{
    fn format_type(&self) -> FormatType;
}
