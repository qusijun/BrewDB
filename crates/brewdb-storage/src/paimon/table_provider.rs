use std::sync::Arc;

use crate::storage::StorageError;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::SchemaExt;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, dml::InsertOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use paimon::DataSplit;
use paimon::spec::{Predicate, TableSchema as PaimonTableSchema};
use paimon::table::Table as PaimonTable;

use super::engine::storage_scan_error;
use super::filter::{filter_predicates, filter_pushdown_status};
use super::reader::PaimonScanExec;
use super::writer::{PaimonCommitExec, PaimonSinkExec};

#[derive(Debug)]
pub struct PaimonTableProvider {
    table: PaimonTable,
    schema: SchemaRef,
    planned_splits: Option<Vec<DataSplit>>,
}

impl PaimonTableProvider {
    pub fn try_new(
        table: PaimonTable,
        planned_splits: Option<Vec<DataSplit>>,
    ) -> Result<Self, StorageError> {
        let schema = paimon_arrow_schema(table.schema()).map_err(storage_scan_error)?;
        Ok(Self {
            table,
            schema,
            planned_splits,
        })
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
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let splits = match &self.planned_splits {
            Some(splits) => splits.clone(),
            None => {
                let mut read_builder = self.table.new_read_builder();
                if let Some(projection) = projection {
                    let projection = projection
                        .iter()
                        .map(|index| self.schema.field(*index).name().as_str())
                        .collect::<Vec<_>>();
                    read_builder.with_projection(&projection);
                }
                let translated_filters = filter_predicates(self.table.schema().fields(), filters);
                if !translated_filters.is_empty() {
                    read_builder.with_filter(Predicate::and(translated_filters));
                }
                if let Some(limit) = limit {
                    read_builder.with_limit(limit);
                }
                let plan = read_builder
                    .new_scan()
                    .plan()
                    .await
                    .map_err(datafusion_scan_error)?;
                plan.splits().to_vec()
            }
        };
        if splits.is_empty() {
            let schema = project_schema(&self.schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(schema)));
        }

        let projected_schema = project_schema(&self.schema, projection)?;
        let projection = projection.map(|indices| {
            indices
                .iter()
                .map(|index| self.schema.field(*index).name().to_string())
                .collect::<Vec<_>>()
        });
        let filter = {
            let filters = filter_predicates(self.table.schema().fields(), filters);
            (!filters.is_empty()).then(|| Predicate::and(filters))
        };
        Ok(Arc::new(PaimonScanExec::new(
            self.table.clone(),
            projected_schema,
            splits,
            projection,
            filter,
            limit,
            state.config_options().execution.target_partitions,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn datafusion::catalog::Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(format!(
                "{insert_op} is not implemented for Paimon tables"
            )));
        }
        input
            .schema()
            .logically_equivalent_names_and_types(&self.schema)
            .map_err(datafusion_scan_error)?;
        let sink = Arc::new(PaimonSinkExec::new(input, self.table.clone()));
        let gather = Arc::new(CoalescePartitionsExec::new(sink));
        Ok(Arc::new(PaimonCommitExec::new(gather, self.table.clone())))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                match filter_pushdown_status(
                    self.table.schema().fields(),
                    self.table.schema().partition_keys(),
                    filter,
                ) {
                    Some((_predicate, exact)) => {
                        if exact {
                            TableProviderFilterPushDown::Exact
                        } else {
                            TableProviderFilterPushDown::Inexact
                        }
                    }
                    None => TableProviderFilterPushDown::Unsupported,
                }
            })
            .collect())
    }
}

pub(crate) fn datafusion_scan_error(error: impl ToString) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
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
            Ok(datafusion::arrow::datatypes::Field::new(
                field.name(),
                paimon::arrow::paimon_type_to_arrow(field.data_type())?,
                field.data_type().is_nullable(),
            ))
        })
        .collect::<paimon::Result<Vec<_>>>()?;
    Ok(Arc::new(datafusion::arrow::datatypes::Schema::new(fields)))
}
