//! Shared query profile data model.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    Summary,
    Dev,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricCategory {
    Rows,
    Bytes,
    Timing,
    Uncategorized,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricScope {
    Operator,
    Fragment,
    Query,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Label {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RatioMergeStrategy {
    AddPartAddTotal,
    AddPartSetTotal,
    SetPartAddTotal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruningMetrics {
    pub pruned: u64,
    pub matched: u64,
    pub fully_matched: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricValue {
    OutputRows(u64),
    ElapsedCompute(u64),
    SpillCount(u64),
    SpilledBytes(u64),
    OutputBytes(u64),
    OutputBatches(u64),
    SpilledRows(u64),
    CurrentMemoryUsage(u64),
    PeakMemoryUsage(u64),
    StorageFilesRead(u64),
    StorageRowsRead(u64),
    StorageBytesRead(u64),
    Count {
        name: String,
        count: u64,
    },
    Gauge {
        name: String,
        gauge: u64,
    },
    Time {
        name: String,
        time: u64,
    },
    StartTimestamp(Option<i64>),
    EndTimestamp(Option<i64>),
    PruningMetrics {
        name: String,
        pruning_metrics: PruningMetrics,
    },
    Ratio {
        name: String,
        part: u64,
        total: u64,
        display_raw_values: bool,
        merge_strategy: RatioMergeStrategy,
    },
}

impl MetricValue {
    pub const OUTPUT_ROWS: &'static str = "output_rows";
    pub const ELAPSED_COMPUTE: &'static str = "elapsed_compute";
    pub const SPILL_COUNT: &'static str = "spill_count";
    pub const SPILLED_BYTES: &'static str = "spilled_bytes";
    pub const OUTPUT_BYTES: &'static str = "output_bytes";
    pub const OUTPUT_BATCHES: &'static str = "output_batches";
    pub const SPILLED_ROWS: &'static str = "spilled_rows";
    pub const CURRENT_MEMORY_USAGE: &'static str = "mem_used";
    pub const PEAK_MEMORY_USAGE: &'static str = "peak_mem_used";
    pub const STORAGE_FILES_READ: &'static str = "storage_files";
    pub const STORAGE_ROWS_READ: &'static str = "storage_rows";
    pub const STORAGE_BYTES_READ: &'static str = "storage_bytes";
    pub const START_TIMESTAMP: &'static str = "start_timestamp";
    pub const END_TIMESTAMP: &'static str = "end_timestamp";

    pub fn name(&self) -> &str {
        match self {
            Self::OutputRows(_) => Self::OUTPUT_ROWS,
            Self::ElapsedCompute(_) => Self::ELAPSED_COMPUTE,
            Self::SpillCount(_) => Self::SPILL_COUNT,
            Self::SpilledBytes(_) => Self::SPILLED_BYTES,
            Self::OutputBytes(_) => Self::OUTPUT_BYTES,
            Self::OutputBatches(_) => Self::OUTPUT_BATCHES,
            Self::SpilledRows(_) => Self::SPILLED_ROWS,
            Self::CurrentMemoryUsage(_) => Self::CURRENT_MEMORY_USAGE,
            Self::PeakMemoryUsage(_) => Self::PEAK_MEMORY_USAGE,
            Self::StorageFilesRead(_) => Self::STORAGE_FILES_READ,
            Self::StorageRowsRead(_) => Self::STORAGE_ROWS_READ,
            Self::StorageBytesRead(_) => Self::STORAGE_BYTES_READ,
            Self::Count { name, .. }
            | Self::Gauge { name, .. }
            | Self::Time { name, .. }
            | Self::PruningMetrics { name, .. }
            | Self::Ratio { name, .. } => name,
            Self::StartTimestamp(_) => Self::START_TIMESTAMP,
            Self::EndTimestamp(_) => Self::END_TIMESTAMP,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::OutputRows(value)
            | Self::ElapsedCompute(value)
            | Self::SpillCount(value)
            | Self::SpilledBytes(value)
            | Self::OutputBytes(value)
            | Self::OutputBatches(value)
            | Self::SpilledRows(value)
            | Self::CurrentMemoryUsage(value)
            | Self::PeakMemoryUsage(value) => Some(*value),
            Self::StorageFilesRead(value)
            | Self::StorageRowsRead(value)
            | Self::StorageBytesRead(value) => Some(*value),
            Self::Count { count, .. }
            | Self::Gauge { gauge: count, .. }
            | Self::Time { time: count, .. } => Some(*count),
            Self::StartTimestamp(value) | Self::EndTimestamp(value) => {
                value.and_then(|value| value.try_into().ok())
            }
            Self::PruningMetrics {
                pruning_metrics, ..
            } => Some(
                pruning_metrics
                    .pruned
                    .saturating_add(pruning_metrics.matched)
                    .saturating_add(pruning_metrics.fully_matched),
            ),
            Self::Ratio { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub value: MetricValue,
    pub labels: Vec<Label>,
    pub partition: Option<usize>,
    pub scope: MetricScope,
    pub metric_type: MetricType,
    pub metric_category: Option<MetricCategory>,
}

impl Metric {
    pub fn name(&self) -> &str {
        self.value.name()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseProfile {
    pub name: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperatorProfile {
    pub name: String,
    pub metrics: Vec<Metric>,
    pub children: Vec<OperatorProfile>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FragmentProfile {
    pub fragment_id: String,
    pub worker_id: Option<String>,
    pub kind: String,
    pub elapsed_ms: u64,
    pub metrics: Vec<Metric>,
    pub operators: Vec<OperatorProfile>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryProfile {
    pub query_id: String,
    pub session_id: String,
    pub command_tag: String,
    pub success: bool,
    pub error: Option<String>,
    pub phases: Vec<PhaseProfile>,
    pub metrics: Vec<Metric>,
    pub fragments: Vec<FragmentProfile>,
}

impl QueryProfile {
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FragmentProfile, Label, Metric, MetricCategory, MetricScope, MetricType, MetricValue,
        OperatorProfile, PhaseProfile, QueryProfile,
    };

    #[test]
    fn metric_name_follows_variant() {
        let metric = Metric {
            value: MetricValue::StorageRowsRead(7),
            labels: vec![Label {
                name: "partition".to_owned(),
                value: "0".to_owned(),
            }],
            partition: Some(0),
            scope: MetricScope::Operator,
            metric_type: MetricType::Dev,
            metric_category: Some(MetricCategory::Rows),
        };

        assert_eq!(metric.name(), "storage_rows");
        assert_eq!(MetricValue::STORAGE_ROWS_READ, "storage_rows");
        assert_eq!(metric.value.as_u64(), Some(7));
    }

    #[test]
    fn query_profile_serializes_to_stable_json_shape() {
        let profile = QueryProfile {
            query_id: "q-1".to_owned(),
            session_id: "s-1".to_owned(),
            command_tag: "SELECT".to_owned(),
            success: true,
            error: None,
            phases: vec![PhaseProfile {
                name: "execute".to_owned(),
                elapsed_ms: 12,
            }],
            metrics: vec![Metric {
                value: MetricValue::PeakMemoryUsage(4096),
                labels: vec![],
                partition: None,
                scope: MetricScope::Query,
                metric_type: MetricType::Summary,
                metric_category: Some(MetricCategory::Bytes),
            }],
            fragments: vec![FragmentProfile {
                fragment_id: "0".to_owned(),
                worker_id: Some("worker-0".to_owned()),
                kind: "Root".to_owned(),
                elapsed_ms: 12,
                metrics: vec![Metric {
                    value: MetricValue::OutputRows(1),
                    labels: vec![],
                    partition: None,
                    scope: MetricScope::Fragment,
                    metric_type: MetricType::Summary,
                    metric_category: Some(MetricCategory::Rows),
                }],
                operators: vec![OperatorProfile {
                    name: "ProjectionExec".to_owned(),
                    metrics: vec![Metric {
                        value: MetricValue::OutputRows(1),
                        labels: vec![],
                        partition: None,
                        scope: MetricScope::Operator,
                        metric_type: MetricType::Summary,
                        metric_category: Some(MetricCategory::Rows),
                    }],
                    children: vec![],
                }],
            }],
        };

        let json = serde_json::to_value(profile).unwrap();

        assert_eq!(json["query_id"], "q-1");
        assert_eq!(json["phases"][0]["name"], "execute");
        assert_eq!(json["metrics"][0]["value"]["peak_memory_usage"], 4096);
        assert_eq!(json["metrics"][0]["scope"], "query");
        assert_eq!(
            json["fragments"][0]["metrics"][0]["value"]["output_rows"],
            1
        );
        assert_eq!(json["fragments"][0]["metrics"][0]["scope"], "fragment");
        assert_eq!(
            json["fragments"][0]["operators"][0]["metrics"][0]["value"]["output_rows"],
            1
        );
        assert_eq!(
            json["fragments"][0]["operators"][0]["metrics"][0]["scope"],
            "operator"
        );
    }
}
