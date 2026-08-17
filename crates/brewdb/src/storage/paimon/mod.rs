//! Apache Paimon storage adapter for BrewDB.

use std::sync::Arc;

use crate::storage::StorageEngine;

mod engine;
mod reader;
mod table_provider;
mod table_sink;
mod writer;

pub use engine::{PaimonStorageEngine, PaimonTableEngine};

fn open_paimon_storage_engine() -> Arc<dyn StorageEngine> {
    Arc::new(PaimonStorageEngine)
}

crate::register_storage_engine!("paimon", open_paimon_storage_engine);

#[cfg(test)]
mod tests {
    use crate::catalog::{
        CatalogMode, CreateTableRequest, PaimonSchemaAdapter, StorageFormatSchemaAdapter,
        StorageKind, TableCatalogEntry, TablePath,
    };
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::storage::{StorageEngine, StorageError};
    use arrow::array::Int64Array;
    use datafusion::prelude::SessionContext;
    use paimon::io::FileIO;

    use super::PaimonStorageEngine;

    fn make_table(storage_kind: StorageKind) -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            format!("memory:/brewdb-paimon-test-{}", uuid::Uuid::new_v4()),
            storage_kind,
            CatalogMode::Managed,
        )
    }

    #[test]
    fn paimon_storage_rejects_non_paimon_tables() {
        let storage = PaimonStorageEngine;

        assert!(matches!(
            storage.table_engine(&make_table(StorageKind::Iceberg)),
            Err(StorageError::UnsupportedStorageKind { .. })
        ));
    }

    #[test]
    fn paimon_table_provider_exposes_brewdb_schema() {
        let storage = PaimonStorageEngine;
        let table = make_table(StorageKind::Paimon);
        let engine = storage.table_engine(&table).unwrap();
        let provider = engine.table_provider().unwrap();

        assert_eq!(provider.schema().field(0).name(), "id");
    }

    #[test]
    fn paimon_storage_reuses_catalog_paimon_schema_adapter() {
        let request = CreateTableRequest::new(
            "sales",
            "orders",
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
        )
        .with_primary_keys(["id"])
        .with_bucket_keys(["id"])
        .with_bucket_count(4)
        .with_bucket_function("mod")
        .with_location("memory:/brewdb-paimon-schema-adapter");

        let schema = PaimonSchemaAdapter::build_schema(&request).unwrap();
        let table_schema = paimon::spec::TableSchema::new(0, &schema);

        assert_eq!(table_schema.primary_keys(), &["id".to_string()]);
        assert_eq!(table_schema.bucket_keys(), vec!["id".to_string()]);
        assert_eq!(
            table_schema.options().get("bucket").map(String::as_str),
            Some("4")
        );
        assert_eq!(
            table_schema
                .options()
                .get("bucket-function.type")
                .map(String::as_str),
            Some("mod")
        );
    }

    #[test]
    fn paimon_table_provider_writes_inserted_batches() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let storage = PaimonStorageEngine;
            let table = make_table(StorageKind::Paimon);
            let file_io = FileIO::from_path(&table.table_location)
                .unwrap()
                .build()
                .unwrap();
            file_io
                .mkdirs(&format!("{}/snapshot/", table.table_location))
                .await
                .unwrap();
            file_io
                .mkdirs(&format!("{}/manifest/", table.table_location))
                .await
                .unwrap();
            let engine = storage.table_engine(&table).unwrap();
            let provider = engine.table_provider().unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("orders", provider).unwrap();

            ctx.sql("insert into orders values (cast(1 as int)), (cast(2 as int))")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let batches = ctx
                .sql("select count(*) from orders")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            assert_eq!(count.value(0), 2);
        });
    }
}
