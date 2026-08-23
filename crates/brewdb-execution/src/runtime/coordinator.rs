//! Query coordination for distributed execution.

use std::sync::Arc;

use crate::catalog::{
    CatalogService, CreateDatabaseRequest, CreateTableRequest, TableCatalogEntry,
};
use crate::common::context::QueryContext;
use crate::common::table::TableSchema;
use crate::common::table::{primary_key_names, table_reference_parts};
use crate::planner::CommandPlan;
use crate::planner::distributed::{DistributedFragmentPlan, DistributedPlanRoot};
use crate::planner::{Ddl, DropDatabase, LogicalPlanNode, Show};
use crate::storage::StorageEngine;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_expr::{CreateExternalTable, DdlStatement, DropTable};

use crate::execution::executor::FragmentExecutionEnvelope;
use crate::runtime::FragmentInstance;
use crate::runtime::exchange_service::TransportExchangePageSink;
use crate::runtime::execution_graph::{ExecutionGraph, QueryExecutionHandle, QueryOutput};
use crate::runtime::scheduler::{
    AllAtOnceFragmentScheduler, FragmentScheduler, ResourceManager, StaticResourceManager,
    WorkerInfo,
};
use crate::runtime::transport::{FragmentTransport, LocalFragmentTransport, TransportRegistry};
use crate::runtime::{ExecutionRuntimeError, FragmentSchedulerError};

pub struct QueryCoordinator {
    pub(crate) scheduler: AllAtOnceFragmentScheduler,
    pub(crate) resource_manager: Arc<dyn ResourceManager>,
    pub(crate) transport_registry: Arc<dyn TransportRegistry>,
    pub(crate) storage: Arc<StorageEngine>,
    pub(crate) catalog_service: Option<CatalogService>,
}

impl Default for QueryCoordinator {
    fn default() -> Self {
        let storage = crate::runtime::storage::build_storage_engine();
        Self {
            scheduler: AllAtOnceFragmentScheduler::default(),
            resource_manager: Arc::new(StaticResourceManager::new(vec![WorkerInfo {
                worker_id: uuid::Uuid::nil(),
                endpoint: "rpc://worker-0".to_owned(),
            }])),
            transport_registry: Arc::new(std::collections::BTreeMap::from([(
                "rpc://worker-0".to_owned(),
                Arc::new(LocalFragmentTransport::with_storage(Arc::clone(&storage)))
                    as Arc<dyn FragmentTransport>,
            )])),
            storage,
            catalog_service: None,
        }
    }
}

impl QueryCoordinator {
    pub fn with_storage(storage: Arc<StorageEngine>) -> Self {
        Self {
            scheduler: AllAtOnceFragmentScheduler::default(),
            resource_manager: Arc::new(StaticResourceManager::new(vec![WorkerInfo {
                worker_id: uuid::Uuid::nil(),
                endpoint: "rpc://worker-0".to_owned(),
            }])),
            transport_registry: Arc::new(std::collections::BTreeMap::from([(
                "rpc://worker-0".to_owned(),
                Arc::new(LocalFragmentTransport::with_storage(Arc::clone(&storage)))
                    as Arc<dyn FragmentTransport>,
            )])),
            storage,
            catalog_service: None,
        }
    }

    pub fn with_catalog_service(mut self, catalog_service: CatalogService) -> Self {
        self.catalog_service = Some(catalog_service);
        self
    }

    pub fn with_resource_manager(mut self, resource_manager: Arc<dyn ResourceManager>) -> Self {
        self.resource_manager = resource_manager;
        self
    }

    pub fn with_transport_registry(
        mut self,
        transport_registry: Arc<dyn TransportRegistry>,
    ) -> Self {
        self.transport_registry = transport_registry;
        self
    }

    pub fn with_scheduler(mut self, scheduler: AllAtOnceFragmentScheduler) -> Self {
        self.scheduler = scheduler;
        self
    }

    pub fn resource_manager(&self) -> &dyn ResourceManager {
        self.resource_manager.as_ref()
    }

    pub fn build_execution_graph(
        &self,
        plan: DistributedFragmentPlan,
    ) -> Result<ExecutionGraph, FragmentSchedulerError> {
        let query_context: QueryContext = plan.query_context.clone();
        let execution_graph = ExecutionGraph::from_plan_fragments(query_context, plan.fragments);
        self.scheduler.schedule(
            execution_graph,
            plan.fragment_scan_splits,
            self.resource_manager.as_ref(),
        )
    }

    fn local_fragment_tables(plan: &DistributedFragmentPlan) -> Vec<TableCatalogEntry> {
        plan.table_catalogs.clone()
    }

    fn execution_result_shape(plan: &DistributedFragmentPlan) -> (&str, bool) {
        (plan.command_tag.as_str(), plan.returns_rows)
    }

    fn execute_command(
        &self,
        query_context: QueryContext,
        command: CommandPlan,
    ) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
        self.execute_command_plan(query_context, command)
    }

    pub fn execute_command_plan(
        &self,
        query_context: QueryContext,
        command: CommandPlan,
    ) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
        let catalog_service = self.catalog_service.as_ref().ok_or_else(|| {
            ExecutionRuntimeError::RuntimeInitFailed {
                reason: "catalog service is required for command execution".to_owned(),
            }
        })?;
        match command {
            CommandPlan::Ddl(statement) => execute_ddl(catalog_service, query_context, statement),
            CommandPlan::Extension(node) => {
                execute_brewdb_extension(catalog_service, query_context, node)
            }
            CommandPlan::Statement(statement) => Err(ExecutionRuntimeError::InvalidPlan {
                reason: format!("unsupported statement command: {statement:?}"),
            }),
        }
    }

    fn single_node_worker_id(&self) -> Option<uuid::Uuid> {
        let workers = self.resource_manager().workers();
        match workers.as_slice() {
            [worker] => Some(worker.worker_id),
            _ => None,
        }
    }

    pub fn is_single_node(&self) -> bool {
        self.single_node_worker_id().is_some()
    }

    pub fn build_fragment_instances(
        &self,
        distributed_plan: DistributedFragmentPlan,
    ) -> Result<Vec<FragmentInstance>, ExecutionRuntimeError> {
        let table_catalogs = Self::local_fragment_tables(&distributed_plan);
        let exchanges = distributed_plan.exchanges.clone();
        let execution_graph = self
            .build_execution_graph(distributed_plan)
            .map_err(|err| ExecutionRuntimeError::InvalidPlan {
                reason: err.to_string(),
            })?;
        self.build_fragment_instances_from_graph(execution_graph, exchanges, table_catalogs)
    }

    fn build_fragment_instances_from_graph(
        &self,
        execution_graph: ExecutionGraph,
        exchanges: Vec<crate::planner::distributed::exchange::ExchangeNode>,
        table_catalogs: Vec<TableCatalogEntry>,
    ) -> Result<Vec<FragmentInstance>, ExecutionRuntimeError> {
        let exchange_channels =
            crate::runtime::exchange::build_exchange_channels(&exchanges, &execution_graph)
                .map_err(|err| ExecutionRuntimeError::InvalidPlan {
                    reason: err.to_string(),
                })?;
        let query_context = execution_graph.query_context.clone();

        Ok(execution_graph
            .instances
            .into_iter()
            .map(|instance| {
                let fragment_id = instance.fragment_id();
                FragmentInstance {
                    query_context: query_context.clone(),
                    exchange_inputs: exchange_channels
                        .iter()
                        .filter(|channel| channel.target_fragment_id == fragment_id)
                        .cloned()
                        .collect(),
                    exchange_outputs: exchange_channels
                        .iter()
                        .filter(|channel| channel.source_fragment_id == fragment_id)
                        .cloned()
                        .collect(),
                    table_catalogs: table_catalogs.clone(),
                    ..instance
                }
            })
            .collect())
    }

    fn execute_fragment_instances(
        &self,
        query_context: QueryContext,
        distributed_plan: DistributedFragmentPlan,
        instances: Vec<FragmentInstance>,
    ) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
        let output = Arc::new(QueryOutput::default());
        let (command_tag, returns_rows) = Self::execution_result_shape(&distributed_plan);
        std::thread::scope(|scope| {
            let mut joins = Vec::new();
            for instance in instances {
                let transport_registry = Arc::clone(&self.transport_registry);
                let result_batch_sink = Arc::clone(&output)
                    as Arc<dyn crate::runtime::exchange_service::ResultBatchSink>;
                joins.push(scope.spawn(move || {
                    let is_root_fragment = instance.fragment().kind
                        == crate::planner::distributed::PlanFragmentKind::Root;
                    let worker_id = instance.worker_id;
                    let endpoint = instance.endpoint.clone();
                    let page_sink = Arc::new(TransportExchangePageSink::new(Arc::clone(
                        &transport_registry,
                    )));
                    let client = transport_registry.transport(&endpoint).map_err(|err| {
                        ExecutionRuntimeError::InvalidPlan {
                            reason: err.to_string(),
                        }
                    })?;
                    let mut envelope =
                        FragmentExecutionEnvelope::new(instance).with_exchange_page_sink(page_sink);
                    if is_root_fragment && returns_rows {
                        envelope = envelope.with_result_batch_sink(result_batch_sink);
                    }
                    client
                        .execute_fragment(worker_id, envelope)
                        .map_err(|err| ExecutionRuntimeError::InvalidPlan {
                            reason: err.to_string(),
                        })?;
                    Ok::<_, ExecutionRuntimeError>(())
                }));
            }

            for join in joins {
                join.join()
                    .map_err(|_| ExecutionRuntimeError::RuntimeInitFailed {
                        reason: "fragment instance execution thread panicked".to_owned(),
                    })??;
            }
            Ok::<_, ExecutionRuntimeError>(())
        })?;

        Ok(QueryExecutionHandle {
            query_context,
            command_tag: command_tag.to_owned(),
            returns_rows,
            output,
        })
    }

    pub fn execute_query(
        &self,
        query_context: QueryContext,
        distributed_plan: DistributedFragmentPlan,
    ) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
        if let DistributedPlanRoot::Command(command) = distributed_plan.root.clone() {
            return self.execute_command(query_context, command);
        }
        let instances = self.build_fragment_instances(distributed_plan.clone())?;
        self.execute_fragment_instances(query_context, distributed_plan, instances)
    }
}

fn execute_ddl(
    catalog_service: &CatalogService,
    query_context: QueryContext,
    statement: DdlStatement,
) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
    match statement {
        DdlStatement::CreateExternalTable(statement) => {
            create_table(catalog_service, query_context, statement)
        }
        DdlStatement::DropTable(statement) => drop_table(catalog_service, query_context, statement),
        _ => Err(ExecutionRuntimeError::InvalidPlan {
            reason: format!("unsupported DataFusion DDL command: {statement:?}"),
        }),
    }
}

fn execute_brewdb_extension(
    catalog_service: &CatalogService,
    query_context: QueryContext,
    node: LogicalPlanNode,
) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
    match node {
        LogicalPlanNode::Ddl(Ddl::CreateDatabase(plan)) => {
            let catalog = catalog_service
                .open_catalog(&plan.catalog_name)
                .map_err(map_catalog_error)?;
            catalog
                .create_database(CreateDatabaseRequest::new(plan.database_name))
                .map_err(map_catalog_error)?;
            Ok(QueryExecutionHandle::command(
                query_context,
                "CREATE DATABASE",
            ))
        }
        LogicalPlanNode::Ddl(Ddl::DropDatabase(DropDatabase {
            catalog_name,
            database_name,
        })) => {
            let catalog = catalog_service
                .open_catalog(&catalog_name)
                .map_err(map_catalog_error)?;
            catalog
                .drop_database(&database_name)
                .map_err(map_catalog_error)?;
            Ok(QueryExecutionHandle::command(
                query_context,
                "DROP DATABASE",
            ))
        }
        LogicalPlanNode::Show(Show::Catalogs) => {
            let catalogs = catalog_service
                .list_catalogs()
                .map_err(map_catalog_error)?
                .into_iter()
                .map(|entry| entry.path.catalog().to_owned())
                .collect::<Vec<_>>();
            build_show_handle(query_context, "SHOW CATALOGS", "catalog_name", catalogs)
        }
        LogicalPlanNode::Show(Show::Databases { catalog_name }) => {
            let databases = catalog_service
                .list_databases(&catalog_name)
                .map_err(map_catalog_error)?
                .into_iter()
                .map(|entry| entry.path.database().to_owned())
                .collect::<Vec<_>>();
            build_show_handle(query_context, "SHOW DATABASES", "database_name", databases)
        }
        LogicalPlanNode::Show(Show::Tables {
            catalog_name,
            database_name,
        }) => {
            let tables = catalog_service
                .list_tables(&catalog_name, &database_name)
                .map_err(map_catalog_error)?
                .into_iter()
                .map(|entry| entry.path.table().to_owned())
                .collect::<Vec<_>>();
            build_show_handle(query_context, "SHOW TABLES", "table_name", tables)
        }
    }
}

fn create_table(
    catalog_service: &CatalogService,
    query_context: QueryContext,
    statement: CreateExternalTable,
) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
    let parts = table_reference_parts(&statement.name).map_err(map_common_error)?;
    let catalog = catalog_service
        .open_catalog(&parts.catalog_name)
        .map_err(map_catalog_error)?;
    let mut table_schema =
        TableSchema::from_arrow_schema(statement.schema.as_arrow()).map_err(|error| {
            ExecutionRuntimeError::InvalidPlan {
                reason: error.to_string(),
            }
        })?;
    table_schema.primary_keys =
        primary_key_names(statement.schema.as_arrow(), &statement.constraints);
    if table_schema.partition_keys.is_empty() {
        table_schema.partition_keys = statement.table_partition_cols.clone();
    }
    let mut request = CreateTableRequest::new(parts.database_name, parts.table_name, table_schema)
        .with_options(statement.options);
    if !statement.location.is_empty() {
        request = request.with_location(statement.location);
    }
    catalog.create_table(request).map_err(map_catalog_error)?;
    Ok(QueryExecutionHandle::command(query_context, "CREATE TABLE"))
}

fn drop_table(
    catalog_service: &CatalogService,
    query_context: QueryContext,
    statement: DropTable,
) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
    let parts = table_reference_parts(&statement.name).map_err(map_common_error)?;
    let catalog = catalog_service
        .open_catalog(&parts.catalog_name)
        .map_err(map_catalog_error)?;
    catalog
        .drop_table(&parts.database_name, &parts.table_name)
        .map_err(map_catalog_error)?;
    Ok(QueryExecutionHandle::command(query_context, "DROP TABLE"))
}

fn build_show_handle(
    query_context: QueryContext,
    command_tag: impl Into<String>,
    column_name: &str,
    values: Vec<String>,
) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
    let mut handle = QueryExecutionHandle::query(query_context);
    handle.command_tag = command_tag.into();
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        ArrowDataType::Utf8,
        false,
    )]));
    let array: ArrayRef = Arc::new(StringArray::from(values));
    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|error| {
        ExecutionRuntimeError::InvalidPlan {
            reason: error.to_string(),
        }
    })?;
    handle.output.push_result(batch)?;
    Ok(handle)
}

fn map_catalog_error(error: crate::catalog::CatalogError) -> ExecutionRuntimeError {
    ExecutionRuntimeError::CatalogError {
        reason: error.to_string(),
    }
}

fn map_common_error(error: crate::common::errors::CommonError) -> ExecutionRuntimeError {
    ExecutionRuntimeError::InvalidPlan {
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::common::context::QueryContext;
    use crate::planner::CommandTag;
    use crate::planner::distributed::{
        DistributedFragmentPlan, DistributedPlanRoot, FragmentScanSplits, PlanFragment,
        PlanFragmentId, PlanFragmentKind,
    };

    use super::QueryCoordinator;
    use crate::runtime::scheduler::{StaticResourceManager, WorkerInfo};
    use crate::storage::{TableScanSplit, TableScanSplitGroup};

    #[test]
    fn coordinator_builds_execution_graph_with_worker_and_scan_splits() {
        let worker_id = uuid::Uuid::new_v4();
        let fragment_id = PlanFragmentId(0);
        let split = TableScanSplit::new("managed_paimon_catalog.brewdb.t", 0)
            .with_locations(vec!["file:///tmp/t/part-1.csv".to_owned()]);
        let plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![PlanFragment {
                fragment_id,
                kind: PlanFragmentKind::Source,
                root: None,
                local_plan: None,
            }],
            fragment_scan_splits: vec![FragmentScanSplits {
                fragment_id,
                table_scan_splits: TableScanSplitGroup::new(vec![split.clone()]),
            }],
            exchanges: vec![],
        };

        let graph = QueryCoordinator::default()
            .with_resource_manager(std::sync::Arc::new(StaticResourceManager::new(vec![
                WorkerInfo {
                    worker_id,
                    endpoint: "rpc://worker-1".to_owned(),
                },
            ])))
            .build_execution_graph(plan)
            .unwrap();

        assert_eq!(graph.fragments.len(), 1);
        assert_eq!(graph.instances.len(), 1);
        assert_eq!(graph.instances[0].worker_id, worker_id);
        assert_eq!(graph.instances[0].endpoint, "rpc://worker-1");
        assert_eq!(graph.instances[0].table_scan_splits.splits, vec![split]);
    }
}
