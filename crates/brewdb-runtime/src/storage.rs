//! Runtime-owned storage assembly.

use std::sync::Arc;

use brewdb_storage::StorageEngine;
use brewdb_storage_paimon as _;

pub fn build_storage_engine() -> Arc<dyn StorageEngine> {
    brewdb_storage::open_storage_engine().expect("storage engine registry must be valid")
}
