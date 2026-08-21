//! BrewDB storage contracts.

mod errors;
pub mod file;
pub mod memory;
pub mod paimon;
mod storage_engine;

pub use errors::StorageError;
pub use inventory;
pub use storage_engine::{
    open_storage_engine, StorageEngine, StorageEngineRegistration, TableEngine, TableEngineFactory,
};

#[cfg(test)]
mod tests {
    use crate::catalog::StorageKind;

    #[test]
    fn storage_kind_string_mapping_is_stable() {
        assert_eq!(StorageKind::Paimon.as_str(), "paimon");
        assert_eq!(StorageKind::Iceberg.as_str(), "iceberg");
        assert_eq!(StorageKind::File.as_str(), "file");
        assert_eq!(StorageKind::Memory.as_str(), "memory");
    }
}
