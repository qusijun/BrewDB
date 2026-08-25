//! Runtime query profile collection.

use std::time::Instant;

use crate::common::context::QueryContext;
use crate::common::profile::{
    FragmentProfile, Label, Metric, MetricCategory, MetricScope, MetricType, MetricValue,
    OperatorProfile, PhaseProfile, PruningMetrics, QueryProfile, RatioMergeStrategy,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{
    Metric as DataFusionMetric, MetricCategory as DataFusionMetricCategory,
    MetricType as DataFusionMetricType, MetricValue as DataFusionMetricValue,
    MetricsSet as DataFusionMetricsSet,
};
use tracing::info;

pub struct QueryProfiler {
    query_context: QueryContext,
    command_tag: String,
    phases: Vec<PhaseProfile>,
    metrics: Vec<Metric>,
    fragments: Vec<FragmentProfile>,
}

impl QueryProfiler {
    pub fn new(query_context: QueryContext, command_tag: impl Into<String>) -> Self {
        Self {
            query_context,
            command_tag: command_tag.into(),
            phases: vec![],
            metrics: vec![],
            fragments: vec![],
        }
    }

    pub fn scoped_phase(&mut self, name: impl Into<String>) -> PhaseGuard<'_> {
        PhaseGuard {
            profiler: self,
            name: name.into(),
            start: Instant::now(),
        }
    }

    pub fn record_fragment(&mut self, fragment: FragmentProfile) {
        self.fragments.push(fragment);
    }

    pub fn record_query_metric(&mut self, metric: Metric) {
        self.metrics.push(metric);
    }

    pub fn finish_success(self) -> QueryProfile {
        self.finish(true, None)
    }

    pub fn finish_error(self, error: impl Into<String>) -> QueryProfile {
        self.finish(false, Some(error.into()))
    }

    pub fn emit_json_profile(profile: &QueryProfile) {
        match profile.to_json_string() {
            Ok(profile_json) => {
                info!(
                    target: "brewdb.profile",
                    profile = profile_json.as_str(),
                    "query profile"
                );
            }
            Err(error) => {
                info!(
                    target: "brewdb.profile",
                    error = %error,
                    "query profile serialization failed"
                );
            }
        }
    }

    fn finish(self, success: bool, error: Option<String>) -> QueryProfile {
        QueryProfile {
            query_id: self.query_context.query_id.to_string(),
            session_id: self.query_context.session_id.to_string(),
            command_tag: self.command_tag,
            success,
            error,
            phases: self.phases,
            metrics: self.metrics,
            fragments: self.fragments,
        }
    }

    pub fn record_phase(&mut self, name: impl Into<String>, elapsed_ms: u64) {
        self.phases.push(PhaseProfile {
            name: name.into(),
            elapsed_ms,
        });
    }
}

pub struct PhaseGuard<'a> {
    profiler: &'a mut QueryProfiler,
    name: String,
    start: Instant,
}

impl Drop for PhaseGuard<'_> {
    fn drop(&mut self) {
        self.profiler
            .record_phase(self.name.clone(), self.start.elapsed().as_millis() as u64);
    }
}

pub fn operator_profile_from_execution_plan(plan: &dyn ExecutionPlan) -> OperatorProfile {
    let metrics = plan
        .metrics()
        .map(|metrics| serialize_metrics(metrics))
        .unwrap_or_default();
    let children = plan
        .children()
        .into_iter()
        .map(|child| operator_profile_from_execution_plan(child.as_ref()))
        .collect();

    OperatorProfile {
        name: execution_plan_name(plan),
        metrics,
        children,
    }
}

fn execution_plan_name(plan: &dyn ExecutionPlan) -> String {
    let debug = format!("{plan:?}");
    debug
        .split_once(' ')
        .map(|(name, _)| name.to_owned())
        .unwrap_or(debug)
}

fn serialize_metrics(metrics: DataFusionMetricsSet) -> Vec<Metric> {
    metrics
        .aggregate_by_name()
        .sorted_for_display()
        .into_iter()
        .map(|metric| serialize_metric(metric.as_ref()))
        .collect()
}

fn serialize_metric(metric: &DataFusionMetric) -> Metric {
    Metric {
        value: serialize_metric_value(metric.value()),
        labels: metric
            .labels()
            .iter()
            .map(|label| Label {
                name: label.name().to_owned(),
                value: label.value().to_owned(),
            })
            .collect(),
        partition: metric.partition(),
        scope: MetricScope::Operator,
        metric_type: serialize_metric_type(metric.metric_type()),
        metric_category: metric.metric_category().map(serialize_metric_category),
    }
}

fn serialize_metric_type(metric_type: DataFusionMetricType) -> MetricType {
    match metric_type {
        DataFusionMetricType::Summary => MetricType::Summary,
        DataFusionMetricType::Dev => MetricType::Dev,
    }
}

fn serialize_metric_category(metric_category: DataFusionMetricCategory) -> MetricCategory {
    match metric_category {
        DataFusionMetricCategory::Rows => MetricCategory::Rows,
        DataFusionMetricCategory::Bytes => MetricCategory::Bytes,
        DataFusionMetricCategory::Timing => MetricCategory::Timing,
        DataFusionMetricCategory::Uncategorized => MetricCategory::Uncategorized,
    }
}

fn serialize_metric_value(metric_value: &DataFusionMetricValue) -> MetricValue {
    match metric_value {
        DataFusionMetricValue::OutputRows(value) => MetricValue::OutputRows(value.value() as u64),
        DataFusionMetricValue::ElapsedCompute(value) => {
            MetricValue::ElapsedCompute(value.value() as u64)
        }
        DataFusionMetricValue::SpillCount(value) => MetricValue::SpillCount(value.value() as u64),
        DataFusionMetricValue::SpilledBytes(value) => {
            MetricValue::SpilledBytes(value.value() as u64)
        }
        DataFusionMetricValue::OutputBytes(value) => MetricValue::OutputBytes(value.value() as u64),
        DataFusionMetricValue::OutputBatches(value) => {
            MetricValue::OutputBatches(value.value() as u64)
        }
        DataFusionMetricValue::SpilledRows(value) => MetricValue::SpilledRows(value.value() as u64),
        DataFusionMetricValue::CurrentMemoryUsage(value) => {
            MetricValue::CurrentMemoryUsage(value.value() as u64)
        }
        DataFusionMetricValue::Count { name, count } => match name.as_ref() {
            name if name == MetricValue::STORAGE_FILES_READ => {
                MetricValue::StorageFilesRead(count.value() as u64)
            }
            name if name == MetricValue::STORAGE_ROWS_READ => {
                MetricValue::StorageRowsRead(count.value() as u64)
            }
            name if name == MetricValue::STORAGE_BYTES_READ => {
                MetricValue::StorageBytesRead(count.value() as u64)
            }
            _ => MetricValue::Count {
                name: name.to_string(),
                count: count.value() as u64,
            },
        },
        DataFusionMetricValue::Gauge { name, gauge } => MetricValue::Gauge {
            name: name.to_string(),
            gauge: gauge.value() as u64,
        },
        DataFusionMetricValue::Time { name, time } => MetricValue::Time {
            name: name.to_string(),
            time: time.value() as u64,
        },
        DataFusionMetricValue::StartTimestamp(timestamp) => MetricValue::StartTimestamp(
            timestamp
                .value()
                .and_then(|timestamp| timestamp.timestamp_nanos_opt()),
        ),
        DataFusionMetricValue::EndTimestamp(timestamp) => MetricValue::EndTimestamp(
            timestamp
                .value()
                .and_then(|timestamp| timestamp.timestamp_nanos_opt()),
        ),
        DataFusionMetricValue::PruningMetrics {
            name,
            pruning_metrics,
        } => MetricValue::PruningMetrics {
            name: name.to_string(),
            pruning_metrics: PruningMetrics {
                pruned: pruning_metrics.pruned() as u64,
                matched: pruning_metrics.matched() as u64,
                fully_matched: pruning_metrics.fully_matched() as u64,
            },
        },
        DataFusionMetricValue::Ratio {
            name,
            ratio_metrics,
        } => MetricValue::Ratio {
            name: name.to_string(),
            part: ratio_metrics.part() as u64,
            total: ratio_metrics.total() as u64,
            display_raw_values: ratio_metrics.display_raw_values(),
            merge_strategy: match ratio_metrics.merge_strategy() {
                datafusion::physical_plan::metrics::RatioMergeStrategy::AddPartAddTotal => {
                    RatioMergeStrategy::AddPartAddTotal
                }
                datafusion::physical_plan::metrics::RatioMergeStrategy::AddPartSetTotal => {
                    RatioMergeStrategy::AddPartSetTotal
                }
                datafusion::physical_plan::metrics::RatioMergeStrategy::SetPartAddTotal => {
                    RatioMergeStrategy::SetPartAddTotal
                }
            },
        },
        DataFusionMetricValue::Custom { name, value } => MetricValue::Count {
            name: name.to_string(),
            count: value.as_usize() as u64,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::catalog::{CatalogMode, StorageKind, TableCatalogEntry, TablePath};
    use crate::common::column::ColumnField;
    use crate::common::config::ConfigSet;
    use crate::common::context::QueryContext;
    use crate::common::datatype::DataType as BrewDataType;
    use crate::common::profile::{
        FragmentProfile, Metric, MetricCategory, MetricScope, MetricType, MetricValue,
        OperatorProfile,
    };
    use crate::common::table::TableSchema;
    use crate::storage::TableEngineFactory;
    use brewdb_storage::paimon::PaimonTableEngineFactory;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use uuid::Uuid;

    use super::{QueryProfiler, operator_profile_from_execution_plan};

    fn make_file_table(storage_kind: StorageKind) -> TableCatalogEntry {
        let location = format!("/tmp/brewdb-paimon-profile-test-{}", Uuid::new_v4());
        TableCatalogEntry::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", BrewDataType::Int32)]),
            location,
            storage_kind,
            CatalogMode::Managed,
        )
    }

    #[test]
    fn query_profiler_records_phase_and_fragment_profile() {
        let query_context = QueryContext::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "brew",
            Some("brewdb".to_owned()),
            Some("managed_paimon_catalog".to_owned()),
            ConfigSet::new(),
        );
        let mut profiler = QueryProfiler::new(query_context, "SELECT");

        {
            let _phase = profiler.scoped_phase("execute");
        }
        profiler.record_query_metric(Metric {
            value: MetricValue::PeakMemoryUsage(4096),
            labels: vec![],
            partition: None,
            scope: MetricScope::Query,
            metric_type: MetricType::Summary,
            metric_category: Some(MetricCategory::Bytes),
        });
        profiler.record_fragment(FragmentProfile {
            fragment_id: "0".to_owned(),
            worker_id: Some("worker-0".to_owned()),
            kind: "Root".to_owned(),
            elapsed_ms: 1,
            metrics: vec![Metric {
                value: MetricValue::OutputRows(3),
                labels: vec![],
                partition: None,
                scope: MetricScope::Fragment,
                metric_type: MetricType::Summary,
                metric_category: Some(MetricCategory::Rows),
            }],
            operators: vec![OperatorProfile {
                name: "MemoryExec".to_owned(),
                metrics: vec![Metric {
                    value: MetricValue::OutputRows(3),
                    labels: vec![],
                    partition: None,
                    scope: MetricScope::Operator,
                    metric_type: MetricType::Summary,
                    metric_category: Some(MetricCategory::Rows),
                }],
                children: vec![],
            }],
        });

        let profile = profiler.finish_success();
        let json = profile.to_json_string().unwrap();

        assert!(json.contains("\"command_tag\":\"SELECT\""));
        assert_eq!(profile.phases.len(), 1);
        assert_eq!(profile.metrics.len(), 1);
        assert_eq!(profile.fragments.len(), 1);
        assert_eq!(profile.metrics[0].scope, MetricScope::Query);
    }

    #[test]
    fn operator_profile_maps_datafusion_metrics_tree() {
        use std::sync::Arc;

        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let plan = EmptyExec::new(schema);

        let operator = operator_profile_from_execution_plan(&plan);

        assert!(operator.name.contains("EmptyExec"));
        assert!(operator.metrics.is_empty());
        assert!(operator.children.is_empty());
    }

    #[test]
    fn operator_profile_includes_paimon_scan_storage_metrics() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let table = make_file_table(StorageKind::Paimon);
            let _ = fs::remove_dir_all(&table.table_location);
            fs::create_dir_all(format!("{}/snapshot", table.table_location)).unwrap();
            fs::create_dir_all(format!("{}/manifest", table.table_location)).unwrap();

            let engine = PaimonTableEngineFactory
                .create_table_engine(&table)
                .unwrap();
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
            let batches = collect(exec.clone(), ctx.task_ctx()).await.unwrap();
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                2
            );

            let operator = operator_profile_from_execution_plan(exec.as_ref());
            let metric_names: Vec<_> = operator
                .metrics
                .iter()
                .map(|metric| metric.name())
                .collect();

            assert!(metric_names.contains(&MetricValue::STORAGE_BYTES_READ));
            assert!(metric_names.contains(&MetricValue::STORAGE_ROWS_READ));
            assert!(metric_names.contains(&MetricValue::STORAGE_FILES_READ));

            let storage_rows = operator
                .metrics
                .iter()
                .find(|metric| metric.name() == MetricValue::STORAGE_ROWS_READ)
                .map(|metric| &metric.value)
                .unwrap();
            assert!(matches!(
                storage_rows,
                MetricValue::StorageRowsRead(value) if *value >= 2
            ));

            let storage_bytes = operator
                .metrics
                .iter()
                .find(|metric| metric.name() == MetricValue::STORAGE_BYTES_READ)
                .map(|metric| &metric.value)
                .unwrap();
            assert!(matches!(
                storage_bytes,
                MetricValue::StorageBytesRead(value) if *value > 0
            ));

            let _ = fs::remove_dir_all(&table.table_location);
        });
    }
}
