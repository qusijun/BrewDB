//! Runtime-owned storage assembly.

use std::sync::Arc;

use crate::storage::StorageEngine;

pub fn build_storage_engine() -> Arc<StorageEngine> {
    crate::storage::open_storage_engine().expect("storage engine registry must be valid")
}
