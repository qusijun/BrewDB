//! Apache Paimon storage adapter for BrewDB.

use std::sync::{Arc, OnceLock};

use brewdb_catalog::TableCatalogEntry;
use brewdb_common::schema::{DataType, SchemaField};
use brewdb_storage::{StorageEngine, StorageError, TableEngine};
use datafusion::datasource::{MemTable, TableProvider};
use futures::TryStreamExt;
use paimon::catalog::Identifier as PaimonIdentifier;
use paimon::io::FileIO;
use paimon::spec::{
    BigIntType, BooleanType, DataType as PaimonDataType, DateType, DecimalType, DoubleType,
    FloatType, IntType, LocalZonedTimestampType, Schema as PaimonSchema,
    TableSchema as PaimonTableSchema, TimeType, TimestampType, TinyIntType, VarBinaryType,
    VarCharType,
};
use paimon::table::Table as PaimonTable;
use tokio::runtime::Runtime;

fn open_paimon_storage_engine() -> Arc<dyn StorageEngine> {
    Arc::new(PaimonStorageEngine)
}

brewdb_storage::register_storage_engine!("paimon", open_paimon_storage_engine);

pub struct PaimonTableEngine {
    table: TableCatalogEntry,
    tokio_runtime: OnceLock<Runtime>,
}

#[derive(Default)]
pub struct PaimonStorageEngine;

impl StorageEngine for PaimonStorageEngine {
    fn table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        if table.lake_format_kind != brewdb_catalog::LakeFormatKind::Paimon {
            return Err(StorageError::UnsupportedTableFormat {
                format: table.lake_format_kind.as_str().to_owned(),
            });
        }
        Ok(Arc::new(PaimonTableEngine::new(table.clone())))
    }
}

impl PaimonTableEngine {
    pub fn new(table: TableCatalogEntry) -> Self {
        Self {
            table,
            tokio_runtime: OnceLock::new(),
        }
    }

    fn tokio_runtime(&self) -> Result<&Runtime, StorageError> {
        self.tokio_runtime
            .get_or_init(|| Runtime::new().expect("tokio runtime must build"));
        self.tokio_runtime
            .get()
            .ok_or_else(|| StorageError::TableScanFailed {
                reason: "tokio runtime was not initialized".to_owned(),
            })
    }

    fn build_table(&self) -> Result<PaimonTable, StorageError> {
        let file_io = FileIO::from_path(&self.table.table_location)
            .map_err(|err| StorageError::TableScanFailed {
                reason: err.to_string(),
            })?
            .build()
            .map_err(|err| StorageError::TableScanFailed {
                reason: err.to_string(),
            })?;
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
        let runtime = self.tokio_runtime()?;
        let table = self.build_table()?;
        let arrow_schema = self
            .table
            .table_schema
            .to_arrow_schema_ref()
            .map_err(|err| StorageError::TableScanFailed {
                reason: err.to_string(),
            })?;
        let batches = runtime.block_on(async move {
            let scan = table.new_read_builder().new_scan();
            let plan = scan
                .plan()
                .await
                .map_err(|err| StorageError::TableScanFailed {
                    reason: err.to_string(),
                })?;
            let read = table.new_read_builder().new_read().map_err(|err| {
                StorageError::TableScanFailed {
                    reason: err.to_string(),
                }
            })?;
            read.to_arrow(plan.splits())
                .map_err(|err| StorageError::TableScanFailed {
                    reason: err.to_string(),
                })?
                .try_collect::<Vec<_>>()
                .await
                .map_err(|err| StorageError::TableScanFailed {
                    reason: err.to_string(),
                })
        })?;
        let provider = Arc::new(
            MemTable::try_new(arrow_schema, vec![batches]).map_err(|err| {
                StorageError::TableScanFailed {
                    reason: err.to_string(),
                }
            })?,
        );
        Ok(provider)
    }
}

fn build_paimon_schema(table: &TableCatalogEntry) -> Result<PaimonTableSchema, StorageError> {
    let mut builder = PaimonSchema::builder();
    for field in &table.table_schema.fields {
        builder = builder.column(&field.name, brewdb_field_to_paimon_type(field)?);
    }
    for (key, value) in &table.table_options {
        builder = builder.option(key.clone(), value.clone());
    }
    builder = builder.option("path", table.table_location.clone());
    builder
        .build()
        .map_err(|err| StorageError::TableScanFailed {
            reason: err.to_string(),
        })
        .map(|schema| PaimonTableSchema::new(0, &schema))
}

fn brewdb_field_to_paimon_type(field: &SchemaField) -> Result<PaimonDataType, StorageError> {
    let nullable = field.nullable;
    match field.data_type {
        DataType::Boolean => Ok(PaimonDataType::Boolean(BooleanType::with_nullable(
            nullable,
        ))),
        DataType::Int8 => Ok(PaimonDataType::TinyInt(TinyIntType::with_nullable(
            nullable,
        ))),
        DataType::Int16 => Ok(PaimonDataType::SmallInt(
            paimon::spec::SmallIntType::with_nullable(nullable),
        )),
        DataType::Int32 => Ok(PaimonDataType::Int(IntType::with_nullable(nullable))),
        DataType::Int64 => Ok(PaimonDataType::BigInt(BigIntType::with_nullable(nullable))),
        DataType::Float32 => Ok(PaimonDataType::Float(FloatType::with_nullable(nullable))),
        DataType::Double => Ok(PaimonDataType::Double(DoubleType::with_nullable(nullable))),
        DataType::Binary => Ok(PaimonDataType::VarBinary(
            VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH).map_err(|err| {
                StorageError::TableScanFailed {
                    reason: err.to_string(),
                }
            })?,
        )),
        DataType::Date => Ok(PaimonDataType::Date(DateType::with_nullable(nullable))),
        DataType::Time { precision } => Ok(PaimonDataType::Time(
            TimeType::with_nullable(nullable, precision).map_err(|err| {
                StorageError::TableScanFailed {
                    reason: err.to_string(),
                }
            })?,
        )),
        DataType::Timestamp {
            precision,
            with_time_zone,
        } => {
            if with_time_zone {
                Ok(PaimonDataType::LocalZonedTimestamp(
                    LocalZonedTimestampType::with_nullable(nullable, precision).map_err(|err| {
                        StorageError::TableScanFailed {
                            reason: err.to_string(),
                        }
                    })?,
                ))
            } else {
                Ok(PaimonDataType::Timestamp(
                    TimestampType::with_nullable(nullable, precision).map_err(|err| {
                        StorageError::TableScanFailed {
                            reason: err.to_string(),
                        }
                    })?,
                ))
            }
        }
        DataType::Decimal { precision, scale } => Ok(PaimonDataType::Decimal(
            DecimalType::with_nullable(nullable, precision, scale).map_err(|err| {
                StorageError::TableScanFailed {
                    reason: err.to_string(),
                }
            })?,
        )),
        DataType::String => Ok(PaimonDataType::VarChar(
            VarCharType::with_nullable(nullable, u32::MAX).map_err(|err| {
                StorageError::TableScanFailed {
                    reason: err.to_string(),
                }
            })?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use brewdb_catalog::{CatalogMode, LakeFormatKind, TableCatalogEntry, TablePath};
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};
    use brewdb_storage::{StorageEngine, StorageError};

    use super::PaimonStorageEngine;

    fn make_table(lake_format_kind: LakeFormatKind) -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            "file:///tmp/brewdb-paimon-test",
            lake_format_kind,
            CatalogMode::Managed,
        )
    }

    #[test]
    fn paimon_storage_rejects_non_paimon_tables() {
        let storage = PaimonStorageEngine;

        assert!(matches!(
            storage.table_engine(&make_table(LakeFormatKind::Iceberg)),
            Err(StorageError::UnsupportedTableFormat { .. })
        ));
    }
}
