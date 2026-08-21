use std::sync::Arc;

use crate::catalog::{
    CreateTableRequest, PaimonSchemaAdapter, StorageFormatSchemaAdapter, StorageKind,
    TableCatalogEntry,
};
use crate::storage::{StorageError, TableEngine, TableEngineFactory};
use datafusion::datasource::TableProvider;
use paimon::catalog::Identifier as PaimonIdentifier;
use paimon::io::FileIO;
use paimon::spec::TableSchema as PaimonTableSchema;
use paimon::table::Table as PaimonTable;

use super::table_provider::PaimonTableProvider;

pub struct PaimonTableEngine {
    table: TableCatalogEntry,
}

#[derive(Default)]
pub struct PaimonTableEngineFactory;

impl TableEngineFactory for PaimonTableEngineFactory {
    fn create_table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        if table.storage_kind != StorageKind::Paimon {
            return Err(StorageError::UnsupportedStorageKind {
                storage_kind: table.storage_kind.as_str().to_owned(),
            });
        }
        Ok(Arc::new(PaimonTableEngine::new(table.clone())))
    }
}

fn open_paimon_table_engine_factory() -> Arc<dyn TableEngineFactory> {
    Arc::new(PaimonTableEngineFactory)
}

crate::register_table_engine_factory!(StorageKind::Paimon, open_paimon_table_engine_factory);

impl PaimonTableEngine {
    pub fn new(table: TableCatalogEntry) -> Self {
        Self { table }
    }

    fn build_table(&self) -> Result<PaimonTable, StorageError> {
        let file_io = FileIO::from_path(&self.table.table_location)
            .map_err(storage_scan_error)?
            .build()
            .map_err(storage_scan_error)?;
        let schema = build_paimon_schema(&self.table)?;
        let identifier = PaimonIdentifier::new(self.table.path.database(), self.table.path.table());
        Ok(PaimonTable::new(
            file_io,
            identifier,
            self.table.table_location.clone(),
            schema,
            None,
        ))
    }
}

impl TableEngine for PaimonTableEngine {
    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError> {
        Ok(Arc::new(PaimonTableProvider::try_new(self.build_table()?)?))
    }
}

pub(crate) fn storage_scan_error(error: impl ToString) -> StorageError {
    StorageError::TableScanFailed {
        reason: error.to_string(),
    }
}

fn build_paimon_schema(table: &TableCatalogEntry) -> Result<PaimonTableSchema, StorageError> {
    let schema = PaimonSchemaAdapter::build_schema(
        &CreateTableRequest::new(
            table.path.database(),
            table.path.table(),
            table.table_schema.clone(),
        )
        .with_location(table.table_location.clone())
        .with_options(table.table_options.clone()),
    )
    .map_err(storage_scan_error)?;
    Ok(PaimonTableSchema::new(0, &schema))
}
