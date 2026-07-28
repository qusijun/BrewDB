//! Worker runtime shell for task execution, data movement, stage-output writing, and reporting.

mod exchange_buffer_manager;
mod stage_output_writer;
mod task_executor;
mod task_status_reporter;

pub use exchange_buffer_manager::{ExchangeBufferManager, ExchangeChannel, ExchangeReservation};
pub use stage_output_writer::{StageOutputWriteResult, StageOutputWriter, WriteStageOutput};
pub use task_executor::{ExecuteTask, TaskExecutionOutcome, TaskExecutor};
pub use task_status_reporter::{TaskProgressUpdate, TaskResultReport, TaskStatusReporter};
