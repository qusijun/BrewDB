//! BrewDB storage contracts.

mod errors;
pub mod file;
pub mod memory;
pub mod paimon;
pub mod scan;
mod storage_engine;

pub use errors::StorageError;
pub use inventory;
pub use scan::{TableScanSplit, TableScanSplitGroup};
pub use storage_engine::{
    open_storage_engine, StorageEngine, StorageEngineRegistration, TableEngine, TableEngineFactory,
};

#[cfg(test)]
mod tests {
    use crate::catalog::StorageKind;

    #[test]
    fn storage_scan_split_types_are_available_from_storage_layer() {
        let split = crate::storage::scan::TableScanSplit::new("orders", 3)
            .with_locations(["worker-a".to_owned()])
            .with_property("path", "orders/part-3.parquet");
        let group = crate::storage::scan::TableScanSplitGroup::new(vec![split]);

        assert_eq!(group.for_table("orders").len(), 1);
        assert!(group.for_table("lineitem").is_empty());
    }

    #[test]
    fn storage_kind_string_mapping_is_stable() {
        assert_eq!(StorageKind::Paimon.as_str(), "paimon");
        assert_eq!(StorageKind::Iceberg.as_str(), "iceberg");
        assert_eq!(StorageKind::File.as_str(), "file");
        assert_eq!(StorageKind::Memory.as_str(), "memory");
    }
}
