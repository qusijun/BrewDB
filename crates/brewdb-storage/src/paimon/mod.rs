//! Apache Paimon storage adapter for BrewDB.

mod engine;
mod reader;
mod table_provider;
mod table_sink;
mod writer;

pub use engine::{PaimonTableEngine, PaimonTableEngineFactory};

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::catalog::{
        CatalogMode, CreateTableRequest, PaimonSchemaAdapter, StorageFormatSchemaAdapter,
        StorageKind, TableCatalogEntry, TablePath,
    };
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::storage::{DataFileFormat, StorageError, TableEngineFactory};
    use arrow::array::Int64Array;
    use datafusion::prelude::SessionContext;
    use paimon::io::FileIO;

    use super::PaimonTableEngineFactory;

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

    fn make_file_table(storage_kind: StorageKind) -> TableCatalogEntry {
        let location = format!("/tmp/brewdb-paimon-test-{}", uuid::Uuid::new_v4());
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            location,
            storage_kind,
            CatalogMode::Managed,
        )
    }

    #[test]
    fn paimon_table_engine_factory_rejects_non_paimon_tables() {
        let factory = PaimonTableEngineFactory;

        assert!(matches!(
            factory.create_table_engine(&make_table(StorageKind::Iceberg)),
            Err(StorageError::UnsupportedStorageKind { .. })
        ));
    }

    #[test]
    fn paimon_table_provider_exposes_brewdb_schema() {
        let factory = PaimonTableEngineFactory;
        let table = make_table(StorageKind::Paimon);
        let engine = factory.create_table_engine(&table).unwrap();
        let provider = engine.table_provider().unwrap();

        assert_eq!(provider.schema().field(0).name(), "id");
    }

    #[test]
    fn paimon_table_engine_plans_empty_table_as_empty_scan_splits() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let factory = PaimonTableEngineFactory;
            let table = make_file_table(StorageKind::Paimon);
            let _ = fs::remove_dir_all(&table.table_location);
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
            let engine = factory.create_table_engine(&table).unwrap();
            let provider = engine.table_provider().unwrap();
            let scan = datafusion_expr::TableScan::try_new(
                datafusion_common::TableReference::bare("orders"),
                datafusion::datasource::provider_as_source(provider),
                None,
                vec![],
                None,
            )
            .unwrap();

            let splits = engine.plan_scan(&scan).unwrap();

            assert!(splits.is_empty(), "expected empty table to plan no splits");
        });
    }

    #[test]
    fn paimon_adapter_reuses_catalog_paimon_schema_adapter() {
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
            let factory = PaimonTableEngineFactory;
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
            let engine = factory.create_table_engine(&table).unwrap();
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

    #[test]
    fn paimon_table_engine_plans_file_scan_splits() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let factory = PaimonTableEngineFactory;
            let table = make_file_table(StorageKind::Paimon);
            let _ = fs::remove_dir_all(&table.table_location);
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
            let engine = factory.create_table_engine(&table).unwrap();
            let provider = engine.table_provider().unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("orders", provider.clone()).unwrap();

            ctx.sql("insert into orders values (cast(1 as int)), (cast(2 as int))")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let scan = datafusion_expr::TableScan::try_new(
                datafusion_common::TableReference::bare("orders"),
                datafusion::datasource::provider_as_source(provider),
                None,
                vec![],
                None,
            )
            .unwrap();

            let splits = engine.plan_scan(&scan).unwrap();

            assert_eq!(splits.len(), 1);
            assert_eq!(splits.splits[0].data_files.len(), 1);
            assert_eq!(
                splits.splits[0].data_files[0].file_format,
                DataFileFormat::Parquet
            );
            assert!(
                splits.splits[0].partition.is_some(),
                "Paimon scan split must carry partition row bytes"
            );
            assert!(
                splits.splits[0].properties.is_empty(),
                "Paimon scan split must not depend on a process-local plan key"
            );
            assert!(splits.splits[0].data_files[0].path.ends_with(".parquet"));

            let _ = fs::remove_dir_all(&table.table_location);
        });
    }

    #[test]
    fn paimon_table_engine_rebuilds_provider_from_scan_splits() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let factory = PaimonTableEngineFactory;
            let table = make_file_table(StorageKind::Paimon);
            let _ = fs::remove_dir_all(&table.table_location);
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
            let engine = factory.create_table_engine(&table).unwrap();
            let provider = engine.table_provider().unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("orders", provider.clone()).unwrap();

            ctx.sql("insert into orders values (cast(1 as int)), (cast(2 as int))")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let scan = datafusion_expr::TableScan::try_new(
                datafusion_common::TableReference::bare("orders"),
                datafusion::datasource::provider_as_source(provider),
                None,
                vec![],
                None,
            )
            .unwrap();
            let mut splits = engine.plan_scan(&scan).unwrap();
            for split in &mut splits.splits {
                split.properties.clear();
            }

            let assigned_provider = engine.get_table_provider(&splits).unwrap();
            let assigned_ctx = SessionContext::new();
            assigned_ctx
                .register_table("orders", assigned_provider)
                .unwrap();

            let batches = assigned_ctx
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

            let _ = fs::remove_dir_all(&table.table_location);
        });
    }

    #[test]
    fn paimon_table_provider_scan_uses_paimon_scan_exec() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let factory = PaimonTableEngineFactory;
            let table = make_file_table(StorageKind::Paimon);
            let _ = fs::remove_dir_all(&table.table_location);
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
            let engine = factory.create_table_engine(&table).unwrap();
            let provider = engine.table_provider().unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("orders", provider.clone()).unwrap();

            ctx.sql("insert into orders values (cast(1 as int)), (cast(2 as int))")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let exec = provider.scan(&ctx.state(), None, &[], None).await.unwrap();

            assert_eq!(exec.name(), "PaimonScanExec");

            let _ = fs::remove_dir_all(&table.table_location);
        });
    }

    #[test]
    fn paimon_table_provider_writes_vortex_data_files_when_table_option_is_set() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let factory = PaimonTableEngineFactory;
            let table =
                make_file_table(StorageKind::Paimon).with_options([("file.format", "vortex")]);
            let _ = fs::remove_dir_all(&table.table_location);
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
            let engine = factory.create_table_engine(&table).unwrap();
            let provider = engine.table_provider().unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("orders", provider).unwrap();

            ctx.sql("insert into orders values (cast(1 as int)), (cast(2 as int))")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let data_files = data_file_paths(Path::new(&table.table_location));
            assert!(
                data_files
                    .iter()
                    .any(|path| path.extension().is_some_and(|ext| ext == "vortex")),
                "expected at least one vortex data file under {}, got {data_files:?}",
                table.table_location
            );
            assert!(
                !data_files
                    .iter()
                    .any(|path| path.extension().is_some_and(|ext| ext == "parquet")),
                "expected vortex table not to write parquet data files, got {data_files:?}"
            );

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

            let _ = fs::remove_dir_all(&table.table_location);
        });
    }

    fn data_file_paths(path: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        collect_data_file_paths(path, &mut files);
        files.sort();
        files
    }

    fn collect_data_file_paths(path: &Path, files: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_data_file_paths(&path, files);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("data-"))
            {
                files.push(path);
            }
        }
    }
}
