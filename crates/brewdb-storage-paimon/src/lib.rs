//! Apache Paimon storage adapter for BrewDB.

use std::sync::Arc;

use async_trait::async_trait;
use brewdb_catalog::{LakeFormatKind, TableCatalogEntry};
use brewdb_common::{column::ColumnField, datatype::DataType};
use brewdb_storage::{StorageEngine, StorageError, TableEngine};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::{StreamExt, stream};
use paimon::DataSplit;
use paimon::catalog::Identifier as PaimonIdentifier;
use paimon::io::FileIO;
use paimon::spec::{
    BigIntType, BooleanType, DataType as PaimonDataType, DateType, DecimalType, DoubleType,
    FloatType, IntType, LocalZonedTimestampType, Schema as PaimonSchema,
    TableSchema as PaimonTableSchema, TimeType, TimestampType, TinyIntType, VarBinaryType,
    VarCharType,
};
use paimon::table::Table as PaimonTable;

fn open_paimon_storage_engine() -> Arc<dyn StorageEngine> {
    Arc::new(PaimonStorageEngine)
}

brewdb_storage::register_storage_engine!("paimon", open_paimon_storage_engine);

pub struct PaimonTableEngine {
    table: TableCatalogEntry,
}

#[derive(Default)]
pub struct PaimonStorageEngine;

impl StorageEngine for PaimonStorageEngine {
    fn table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        if table.lake_format_kind != LakeFormatKind::Paimon {
            return Err(StorageError::UnsupportedTableFormat {
                format: table.lake_format_kind.as_str().to_owned(),
            });
        }
        Ok(Arc::new(PaimonTableEngine::new(table.clone())))
    }
}

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

#[derive(Debug)]
pub struct PaimonTableProvider {
    table: PaimonTable,
    schema: SchemaRef,
}

impl PaimonTableProvider {
    pub fn try_new(table: PaimonTable) -> Result<Self, StorageError> {
        let schema = paimon_arrow_schema(table.schema()).map_err(storage_scan_error)?;
        Ok(Self { table, schema })
    }
}

#[async_trait]
impl TableProvider for PaimonTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let read_builder = self.table.new_read_builder();
        let plan = read_builder
            .new_scan()
            .plan()
            .await
            .map_err(datafusion_scan_error)?;
        let splits = plan.splits().to_vec();
        if splits.is_empty() {
            let schema = project_schema(&self.schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(schema)));
        }

        let partitions = splits
            .into_iter()
            .map(|split| {
                Arc::new(PaimonPartitionStream {
                    table: self.table.clone(),
                    split,
                    schema: Arc::clone(&self.schema),
                }) as Arc<dyn PartitionStream>
            })
            .collect::<Vec<_>>();
        let exec = StreamingTableExec::try_new(
            Arc::clone(&self.schema),
            partitions,
            projection,
            Vec::new(),
            false,
            limit,
        )?;
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

#[derive(Debug)]
struct PaimonPartitionStream {
    table: PaimonTable,
    split: DataSplit,
    schema: SchemaRef,
}

impl PartitionStream for PaimonPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let result = self
            .table
            .new_read_builder()
            .new_read()
            .map_err(datafusion_scan_error)
            .and_then(|read| {
                read.to_arrow(std::slice::from_ref(&self.split))
                    .map_err(datafusion_scan_error)
            });

        let schema = Arc::clone(&self.schema);
        match result {
            Ok(batch_stream) => Box::pin(RecordBatchStreamAdapter::new(
                schema,
                batch_stream.map(|batch| batch.map_err(datafusion_scan_error)),
            )),
            Err(error) => Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::once(async move { Err(error) }),
            )),
        }
    }
}

fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    projection
        .map(|indices| schema.project(indices).map(Arc::new).map_err(Into::into))
        .unwrap_or_else(|| Ok(Arc::clone(schema)))
}

fn paimon_arrow_schema(schema: &PaimonTableSchema) -> paimon::Result<SchemaRef> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(arrow::datatypes::Field::new(
                field.name(),
                paimon::arrow::paimon_type_to_arrow(field.data_type())?,
                field.data_type().is_nullable(),
            ))
        })
        .collect::<paimon::Result<Vec<_>>>()?;
    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

fn storage_scan_error(error: impl ToString) -> StorageError {
    StorageError::TableScanFailed {
        reason: error.to_string(),
    }
}

fn datafusion_scan_error(error: impl ToString) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
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
        .map_err(storage_scan_error)
        .map(|schema| PaimonTableSchema::new(0, &schema))
}

fn brewdb_field_to_paimon_type(field: &ColumnField) -> Result<PaimonDataType, StorageError> {
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
            VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH)
                .map_err(storage_scan_error)?,
        )),
        DataType::Date => Ok(PaimonDataType::Date(DateType::with_nullable(nullable))),
        DataType::Time { precision } => Ok(PaimonDataType::Time(
            TimeType::with_nullable(nullable, precision).map_err(storage_scan_error)?,
        )),
        DataType::Timestamp {
            precision,
            with_time_zone,
        } => {
            if with_time_zone {
                Ok(PaimonDataType::LocalZonedTimestamp(
                    LocalZonedTimestampType::with_nullable(nullable, precision)
                        .map_err(storage_scan_error)?,
                ))
            } else {
                Ok(PaimonDataType::Timestamp(
                    TimestampType::with_nullable(nullable, precision)
                        .map_err(storage_scan_error)?,
                ))
            }
        }
        DataType::Decimal { precision, scale } => Ok(PaimonDataType::Decimal(
            DecimalType::with_nullable(nullable, precision, scale).map_err(storage_scan_error)?,
        )),
        DataType::String => Ok(PaimonDataType::VarChar(
            VarCharType::with_nullable(nullable, u32::MAX).map_err(storage_scan_error)?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use brewdb_catalog::{CatalogMode, LakeFormatKind, TableCatalogEntry, TablePath};
    use brewdb_common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use brewdb_storage::{StorageEngine, StorageError};

    use super::PaimonStorageEngine;

    fn make_table(lake_format_kind: LakeFormatKind) -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
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

    #[test]
    fn paimon_table_provider_exposes_brewdb_schema() {
        let storage = PaimonStorageEngine;
        let table = make_table(LakeFormatKind::Paimon);
        let engine = storage.table_engine(&table).unwrap();
        let provider = engine.table_provider().unwrap();

        assert_eq!(provider.schema().field(0).name(), "id");
    }
}
