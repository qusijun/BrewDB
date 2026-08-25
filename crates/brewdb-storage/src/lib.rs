//! BrewDB storage engine registry and table engines.

pub mod catalog {
    pub use brewdb_catalog::catalog::*;
}

pub mod common {
    pub use brewdb_common::common::*;
}

mod errors;
pub mod file;
pub mod memory;
pub mod paimon;
pub mod scan;
mod storage_engine;

pub mod storage {
    pub use crate::{
        errors::StorageError,
        scan::{
            BucketDescriptor, DataFileDescriptor, DataFileFormat, DeletionFileDescriptor,
            PartitionDescriptor, RowRangeDescriptor, TableScanSplit, TableScanSplitGroup,
            TableSourceId,
        },
        storage_engine::{
            StorageEngine, StorageEngineRegistration, TableEngine, TableEngineFactory,
            open_storage_engine,
        },
    };
    pub use crate::{file, inventory, memory, paimon, scan};
}

pub use inventory;
pub use storage::*;
