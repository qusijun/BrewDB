//! Runtime query profile collection.
//!
//! Ownership ladder:
//!
//! ```text
//! QueryProfiler
//!   summary_metrics_view: QueryMetricSet
//!   summary_metrics: QueryMetrics
//!   fragment_root: FragmentProfiler
//!
//! FragmentProfiler
//!   summary_metrics_view: FragmentMetricSet
//!   summary_metrics: FragmentMetrics
//!   root_execution_plan: ExecutionPlan
//!   children: FragmentProfiler[]
//!
//! QueryMetricSet / FragmentMetricSet
//!   shared DataFusion ExecutionPlanMetricsSet storage
//!
//! QueryMetrics / FragmentMetrics
//!   concrete metric handles registered into the shared storage
//! ```

use crate::common::context::QueryContext;
use crate::planner::distributed::PlanFragmentId;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, Metric, MetricBuilder, MetricCategory, MetricsSet, Time, Timestamp,
};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug)]
/// Query-level concrete metric handles.
pub struct QueryMetrics {
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
    pub query_elapsed: Time,
}

impl QueryMetrics {
    pub fn new(metrics: &QueryMetricSet) -> Self {
        let started_at = MetricBuilder::new(metrics.metrics())
            .with_type(datafusion::physical_plan::metrics::MetricType::Summary)
            .with_category(MetricCategory::Timing)
            .start_timestamp(0);
        started_at.record();

        let query_elapsed = MetricBuilder::new(metrics.metrics())
            .with_type(datafusion::physical_plan::metrics::MetricType::Summary)
            .with_category(MetricCategory::Timing)
            .subset_time("query_elapsed", 0);

        let ended_at = MetricBuilder::new(metrics.metrics())
            .with_type(datafusion::physical_plan::metrics::MetricType::Summary)
            .with_category(MetricCategory::Timing)
            .end_timestamp(0);

        Self {
            started_at,
            query_elapsed,
            ended_at,
        }
    }

    pub fn finish(&self) {
        self.ended_at.record();
    }
}
#[derive(Clone, Debug, Default)]
/// Shared storage for query-level metrics.
///
/// The set owns the underlying `ExecutionPlanMetricsSet`; `QueryMetrics`
/// registers concrete DataFusion metrics into it.
pub struct QueryMetricSet {
    metrics: ExecutionPlanMetricsSet,
}

impl QueryMetricSet {
    pub fn new() -> Self {
        Self {
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn from_metrics(metrics: MetricsSet) -> Self {
        Self {
            metrics: metrics.into(),
        }
    }

    pub fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    pub fn register(&self, metric: Arc<Metric>) {
        self.metrics.register(metric);
    }

    pub fn inner(&self) -> MetricsSet {
        self.metrics.clone_inner()
    }

    pub fn iter(&self) -> impl Iterator<Item = Arc<Metric>> {
        self.metrics
            .clone_inner()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub fn fmt_indent<W: fmt::Write>(
        &self,
        f: &mut W,
        indent: usize,
        metrics_display: ShowMetrics,
    ) -> fmt::Result {
        // The metric set decides how the collected values are rendered.
        if let Some(metrics) = self.formatted_metrics(metrics_display) {
            write_indent(f, indent)?;
            writeln!(f, "summary_metrics=[{metrics}]")?;
        }
        Ok(())
    }

    fn formatted_metrics(&self, display: ShowMetrics) -> Option<MetricsSet> {
        match display {
            ShowMetrics::None => None,
            ShowMetrics::Aggregated => Some(
                self.inner()
                    .aggregate_by_name()
                    .sorted_for_display()
                    .timestamps_removed(),
            ),
            ShowMetrics::Full => Some(self.inner()),
        }
    }
}
#[derive(Clone, Debug)]
/// Top-level runtime profile for one query.
///
/// It owns query summary metrics plus the fragment tree, but not the physical
/// execution plans themselves.
pub struct QueryProfiler {
    pub query_id: String,
    pub session_id: String,
    pub success: bool,
    pub error: Option<String>,
    pub summary_metrics_view: QueryMetricSet,
    pub summary_metrics: QueryMetrics,
    pub fragment_root: Option<FragmentProfiler>,
}

impl QueryProfiler {
    pub fn new(query_context: QueryContext) -> Self {
        let summary_metrics_view = QueryMetricSet::new();
        let summary_metrics = QueryMetrics::new(&summary_metrics_view);
        Self {
            query_id: query_context.query_id.to_string(),
            session_id: query_context.session_id.to_string(),
            success: false,
            error: None,
            summary_metrics_view,
            summary_metrics,
            fragment_root: None,
        }
    }

    pub fn set_fragment_root(&mut self, fragment_root: FragmentProfiler) {
        self.fragment_root = Some(fragment_root);
    }

    pub fn finish_success(self) -> QueryProfiler {
        self.finish(true, None)
    }

    pub fn finish_error(self, error: impl Into<String>) -> QueryProfiler {
        self.finish(false, Some(error.into()))
    }

    fn finish(mut self, success: bool, error: Option<String>) -> QueryProfiler {
        self.success = success;
        self.error = error;
        self.summary_metrics.finish();
        self
    }

    pub fn fmt_indent<W: fmt::Write>(
        &self,
        f: &mut W,
        metrics_display: ShowMetrics,
    ) -> fmt::Result {
        writeln!(f, "QueryProfiler")?;
        write_indent(f, 1)?;
        writeln!(f, "query_id={}", self.query_id)?;
        write_indent(f, 1)?;
        writeln!(f, "session_id={}", self.session_id)?;
        write_indent(f, 1)?;
        writeln!(f, "success={}", self.success)?;
        if let Some(error) = &self.error {
            write_indent(f, 1)?;
            writeln!(f, "error={error}")?;
        }
        self.summary_metrics_view
            .fmt_indent(f, 1, metrics_display)?;

        if let Some(fragment_root) = &self.fragment_root {
            fragment_root.fmt_indent(f, 1, metrics_display)?;
        }
        Ok(())
    }
}

impl fmt::Display for QueryProfiler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indent(f, ShowMetrics::Aggregated)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Metric display selector used by profiler text rendering.
///
/// This mirrors DataFusion's `ShowMetrics` modes, but stays local because the
/// DataFusion enum is not public.
pub enum ShowMetrics {
    None,
    Aggregated,
    Full,
}

#[derive(Clone, Debug, Default)]
/// Shared storage for fragment-level metrics.
///
/// This is the fragment-side counterpart of `QueryMetricSet`.
pub struct FragmentMetricSet {
    metrics: ExecutionPlanMetricsSet,
}

impl FragmentMetricSet {
    pub fn new(metrics: MetricsSet) -> Self {
        Self {
            metrics: metrics.into(),
        }
    }

    pub fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    pub fn register(&self, metric: Arc<Metric>) {
        self.metrics.register(metric);
    }

    pub fn inner(&self) -> MetricsSet {
        self.metrics.clone_inner()
    }

    pub fn iter(&self) -> impl Iterator<Item = Arc<Metric>> {
        self.metrics
            .clone_inner()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub fn fmt_indent<W: fmt::Write>(
        &self,
        f: &mut W,
        indent: usize,
        metrics_display: ShowMetrics,
    ) -> fmt::Result {
        // The metric set decides how the collected values are rendered.
        if let Some(metrics) = self.formatted_metrics(metrics_display) {
            write_indent(f, indent)?;
            writeln!(f, "summary_metrics=[{metrics}]")?;
        }
        Ok(())
    }

    fn formatted_metrics(&self, display: ShowMetrics) -> Option<MetricsSet> {
        match display {
            ShowMetrics::None => None,
            ShowMetrics::Aggregated => Some(
                self.inner()
                    .aggregate_by_name()
                    .sorted_for_display()
                    .timestamps_removed(),
            ),
            ShowMetrics::Full => Some(self.inner()),
        }
    }
}

#[derive(Debug, Clone)]
/// Fragment-level concrete metric handles.
pub struct FragmentMetrics {
    pub fragment_elapsed: Time,
    pub local_plan_rewrite_elapsed: Time,
    pub started_at: Timestamp,
    pub ended_at: Timestamp,
}

impl FragmentMetrics {
    pub fn new(partition: usize, metrics: &FragmentMetricSet) -> Self {
        let started_at = MetricBuilder::new(metrics.metrics())
            .with_type(datafusion::physical_plan::metrics::MetricType::Summary)
            .with_category(MetricCategory::Timing)
            .start_timestamp(partition);
        started_at.record();

        let fragment_elapsed = MetricBuilder::new(metrics.metrics())
            .with_type(datafusion::physical_plan::metrics::MetricType::Summary)
            .with_category(MetricCategory::Timing)
            .elapsed_compute(partition);

        let local_plan_rewrite_elapsed = MetricBuilder::new(metrics.metrics())
            .with_type(datafusion::physical_plan::metrics::MetricType::Summary)
            .with_category(MetricCategory::Timing)
            .subset_time("local_plan_rewrite_elapsed", partition);

        let ended_at = MetricBuilder::new(metrics.metrics())
            .with_type(datafusion::physical_plan::metrics::MetricType::Summary)
            .with_category(MetricCategory::Timing)
            .end_timestamp(partition);

        Self {
            fragment_elapsed,
            local_plan_rewrite_elapsed,
            started_at,
            ended_at,
        }
    }

    pub fn finish(&self) {
        // Elapsed time is tracked by the caller with a scoped timer; this only
        // seals the end timestamp.
        self.ended_at.record();
    }
}
#[derive(Clone, Debug)]
/// Runtime profile for a single fragment instance.
///
/// The fragment owns its summary metrics and the root `ExecutionPlan` that was
/// executed on the worker, plus any child fragments in the dependency tree.
pub struct FragmentProfiler {
    pub fragment_instance_id: u32,
    pub fragment_id: PlanFragmentId,
    pub worker_id: Option<String>,
    pub summary_metrics_view: FragmentMetricSet,
    pub summary_metrics: FragmentMetrics,
    pub root_execution_plan: Option<Arc<dyn ExecutionPlan>>,
    pub children: Vec<FragmentProfiler>,
}

impl FragmentProfiler {
    pub fn fmt_indent<W: fmt::Write>(
        &self,
        f: &mut W,
        indent: usize,
        metrics_display: ShowMetrics,
    ) -> fmt::Result {
        // FragmentProfiler controls the tree shape; the metric set controls how
        // its collected metrics are rendered.
        write_indent(f, indent)?;
        writeln!(f, "FragmentProfiler")?;
        write_indent(f, indent + 1)?;
        writeln!(f, "fragment_instance_id={}", self.fragment_instance_id)?;
        write_indent(f, indent + 1)?;
        writeln!(f, "fragment_id={}", self.fragment_id.0)?;
        if let Some(worker_id) = &self.worker_id {
            write_indent(f, indent + 1)?;
            writeln!(f, "worker_id={worker_id}")?;
        }
        self.summary_metrics_view
            .fmt_indent(f, indent + 1, metrics_display)?;
        if let Some(plan) = &self.root_execution_plan {
            self.write_execution_plan(f, indent + 1, plan.as_ref(), metrics_display)?;
        }
        for child in &self.children {
            child.fmt_indent(f, indent + 1, metrics_display)?;
        }
        Ok(())
    }

    fn write_execution_plan<W: fmt::Write>(
        &self,
        f: &mut W,
        indent: usize,
        plan: &dyn ExecutionPlan,
        metrics_display: ShowMetrics,
    ) -> fmt::Result {
        // The physical plan subtree is rendered with DataFusion's own display
        // machinery; this type only chooses the metric granularity and wraps
        // the output with profiler indentation.
        let display = match metrics_display {
            ShowMetrics::None => DisplayableExecutionPlan::new(plan),
            ShowMetrics::Aggregated => DisplayableExecutionPlan::with_metrics(plan),
            ShowMetrics::Full => DisplayableExecutionPlan::with_full_metrics(plan),
        };
        let rendered = display.indent(true).to_string();
        self.write_indented_block(f, indent, &rendered)
    }

    fn write_indented_block<W: fmt::Write>(
        &self,
        f: &mut W,
        indent: usize,
        text: &str,
    ) -> fmt::Result {
        let prefix = "  ".repeat(indent);
        for (idx, line) in text.lines().enumerate() {
            if idx > 0 {
                writeln!(f)?;
            }
            write!(f, "{prefix}{line}")?;
        }
        if !text.is_empty() {
            writeln!(f)?;
        }
        Ok(())
    }
}

impl fmt::Display for FragmentProfiler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indent(f, 0, ShowMetrics::Aggregated)
    }
}

fn write_indent<W: fmt::Write>(f: &mut W, indent: usize) -> fmt::Result {
    // Shared indentation helper for profiler text output.
    write!(f, "{:indent$}", "", indent = indent * 2)
}

#[cfg(test)]
mod tests {
    use super::{FragmentMetricSet, FragmentMetrics, FragmentProfiler, QueryProfiler, ShowMetrics};
    use crate::common::context::QueryContext;
    use crate::planner::distributed::PlanFragmentId;
    use datafusion::physical_plan::metrics::{MetricBuilder, MetricsSet};

    fn fragment(fragment_instance_id: u32, fragment_id: u32) -> FragmentProfiler {
        let summary_metrics_view = FragmentMetricSet::new(MetricsSet::new());
        let summary_metrics = FragmentMetrics::new(0, &summary_metrics_view);
        FragmentProfiler {
            fragment_instance_id,
            fragment_id: PlanFragmentId(fragment_id),
            worker_id: None,
            summary_metrics_view,
            summary_metrics,
            root_execution_plan: None,
            children: vec![],
        }
    }

    #[test]
    fn query_profiler_displays_fragments_as_dependency_tree() {
        let mut profiler = QueryProfiler::new(QueryContext::for_test(uuid::Uuid::new_v4()));
        let summary_metrics_view = FragmentMetricSet::new(MetricsSet::new());
        let summary_metrics = FragmentMetrics::new(0, &summary_metrics_view);
        profiler.set_fragment_root(FragmentProfiler {
            fragment_instance_id: 0,
            fragment_id: PlanFragmentId(0),
            worker_id: None,
            summary_metrics_view,
            summary_metrics,
            root_execution_plan: None,
            children: vec![fragment(1, 1), fragment(2, 2)],
        });

        let profile = profiler.finish_success();
        let output = format!("{profile}");

        let root_offset = output.find("fragment_id=0").expect("root fragment");
        let first_child_offset = output.find("fragment_id=1").expect("first child fragment");
        let second_child_offset = output.find("fragment_id=2").expect("second child fragment");
        assert!(root_offset < first_child_offset);
        assert!(root_offset < second_child_offset);
        assert!(output.contains("fragment_instance_id=0"));
        assert!(output.contains("fragment_instance_id=1"));
        assert!(output.contains("fragment_instance_id=2"));
    }

    #[test]
    fn fragment_profiler_fmt_indent_controls_metric_granularity() {
        let summary_metrics_view = FragmentMetricSet::new(MetricsSet::new());
        let summary_metrics = FragmentMetrics::new(0, &summary_metrics_view);
        MetricBuilder::new(summary_metrics_view.metrics())
            .output_rows(3)
            .add(7);
        let fragment = FragmentProfiler {
            fragment_instance_id: 0,
            fragment_id: PlanFragmentId(0),
            worker_id: Some("worker-a".to_owned()),
            summary_metrics_view,
            summary_metrics,
            root_execution_plan: None,
            children: vec![],
        };

        let mut aggregated = String::new();
        fragment
            .fmt_indent(&mut aggregated, 0, ShowMetrics::Aggregated)
            .expect("aggregated profile formatting");
        let mut full = String::new();
        fragment
            .fmt_indent(&mut full, 0, ShowMetrics::Full)
            .expect("full profile formatting");
        let mut none = String::new();
        fragment
            .fmt_indent(&mut none, 0, ShowMetrics::None)
            .expect("none profile formatting");

        assert!(aggregated.contains("output_rows=7"));
        assert!(!aggregated.contains("partition=3"));
        assert!(full.contains("output_rows{partition=3}=7"));
        assert!(!none.contains("summary_metrics="));
    }
}
