//! BrewDB daemon application wiring.

use std::error::Error;
use std::fmt;
use std::net::{TcpListener, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use brewdb_catalog::{CatalogConfig, CatalogError, CatalogService, open_catalog_store};
use brewdb_common::config::{ConfigSet, ConfigView, SystemConfigLoader, global_config_registry};
use brewdb_common::diagnostics::DiagnosticError;
use brewdb_common::errors::CommonError;
use brewdb_execution::runtime::{
    QueryCoordinator, QueryExecutionHandle, SqlDriver, SqlDriverError,
};
use brewdb_frontend::{
    ClientDefaults, FrontendConfig, FrontendError, FrontendResponse, FrontendService,
    MANAGED_PAIMON_CATALOG_NAME, ProtocolRegistry, QueryResultOutput, ResultField,
    SqlExecutionResult, SqlRequest, SqlRequestHandler,
};
use tracing::{error, info};

#[derive(Debug)]
pub enum BrewDbServerError {
    Common(CommonError),
    Catalog(CatalogError),
    Frontend(FrontendError),
    Driver(SqlDriverError),
}

impl fmt::Display for BrewDbServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Common(error) => write!(f, "{error}"),
            Self::Catalog(error) => write!(f, "{error}"),
            Self::Frontend(error) => write!(f, "{error}"),
            Self::Driver(error) => write!(f, "{error}"),
        }
    }
}

impl Error for BrewDbServerError {}

impl DiagnosticError for BrewDbServerError {
    fn error_code(&self) -> brewdb_common::diagnostics::ErrorCode {
        match self {
            Self::Common(error) => error.error_code(),
            Self::Catalog(error) => error.error_code(),
            Self::Frontend(error) => error.error_code(),
            Self::Driver(error) => error.error_code(),
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.server"
    }
}

impl From<CommonError> for BrewDbServerError {
    fn from(value: CommonError) -> Self {
        Self::Common(value)
    }
}

impl From<CatalogError> for BrewDbServerError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}

impl From<FrontendError> for BrewDbServerError {
    fn from(value: FrontendError) -> Self {
        Self::Frontend(value)
    }
}

impl From<SqlDriverError> for BrewDbServerError {
    fn from(value: SqlDriverError) -> Self {
        Self::Driver(value)
    }
}

pub struct BrewDbServer {
    frontend: FrontendService,
    client_defaults: ClientDefaults,
    listen_address: String,
    protocols: ProtocolRegistry,
    sql_driver: SqlDriver,
}

impl BrewDbServer {
    pub fn from_system_config(config: ConfigSet) -> Result<Self, BrewDbServerError> {
        let catalog_config = CatalogConfig::from_config_set(&config)?;
        let frontend_config = FrontendConfig::from_config_set(&config)?;
        let catalog_service = CatalogService::with_config_and_default_managed_paimon_catalog(
            open_catalog_store(&catalog_config),
            config.clone(),
            catalog_config,
            frontend_config.default_catalog(),
        )?;
        let mut server = Self::with_catalog_service(catalog_service, QueryCoordinator::default());
        server.frontend = FrontendService::with_system_settings(config);
        server.client_defaults =
            ClientDefaults::default().with_catalog(frontend_config.default_catalog());
        server.listen_address = frontend_config.pgwire_listen_addr().to_owned();
        Ok(server)
    }

    pub fn from_config_file(path: impl AsRef<Path>) -> Result<Self, BrewDbServerError> {
        Self::from_system_config(load_system_config_file(path)?)
    }

    pub fn with_catalog_service(
        catalog_service: CatalogService,
        coordinator: QueryCoordinator,
    ) -> Self {
        Self {
            frontend: FrontendService::new(),
            client_defaults: ClientDefaults::default().with_catalog(MANAGED_PAIMON_CATALOG_NAME),
            listen_address: "127.0.0.1:5432".to_owned(),
            protocols: ProtocolRegistry::with_builtin_plugins(),
            sql_driver: SqlDriver::new(catalog_service, coordinator),
        }
    }

    pub fn execute_client_request(
        &self,
        request: &SqlRequest,
    ) -> Result<QueryExecutionHandle, BrewDbServerError> {
        self.sql_driver
            .execute(&request.sql, request.query_context.clone())
            .map_err(Into::into)
    }

    pub fn frontend(&self) -> &FrontendService {
        &self.frontend
    }

    pub fn protocols(&self) -> &ProtocolRegistry {
        &self.protocols
    }

    pub fn listen_address(&self) -> &str {
        &self.listen_address
    }

    pub fn with_default_catalog(mut self, catalog_name: impl Into<String>) -> Self {
        self.client_defaults = ClientDefaults::default().with_catalog(catalog_name);
        self
    }

    pub fn serve_protocol(
        self: Arc<Self>,
        protocol_name: &str,
        listener: TcpListener,
    ) -> Result<(), BrewDbServerError> {
        let plugin = self.protocols.plugin(protocol_name).ok_or_else(|| {
            BrewDbServerError::Frontend(FrontendError::UnsupportedProtocolMessage {
                message: format!("protocol plugin `{protocol_name}` is not registered"),
            })
        })?;
        let handler = Arc::clone(&self) as Arc<dyn SqlRequestHandler>;

        for connection in listener.incoming() {
            let stream = connection.map_err(|error| {
                BrewDbServerError::Frontend(FrontendError::UnsupportedProtocolMessage {
                    message: format!("failed to accept frontend connection: {error}"),
                })
            })?;
            let plugin = Arc::clone(&plugin);
            let frontend = self.frontend.clone();
            let defaults = self.client_defaults.clone();
            let handler = Arc::clone(&handler);
            std::thread::spawn(move || {
                if let Err(error) = plugin.serve_connection(stream, frontend, defaults, handler) {
                    error!(
                        target: "brewdb.server",
                        protocol = plugin.protocol_name(),
                        error = %error,
                        "frontend connection failed"
                    );
                    eprintln!("frontend connection failed: {error}");
                }
            });
        }
        Ok(())
    }

    pub fn serve_tcp(
        self: Arc<Self>,
        protocol_name: &str,
        address: impl ToSocketAddrs,
    ) -> Result<(), BrewDbServerError> {
        let listener = TcpListener::bind(address).map_err(|error| {
            BrewDbServerError::Frontend(FrontendError::UnsupportedProtocolMessage {
                message: format!("failed to bind frontend listener: {error}"),
            })
        })?;
        let local_addr = listener.local_addr().map(|addr| addr.to_string()).ok();
        info!(
            target: "brewdb.server",
            protocol = protocol_name,
            listen_addr = local_addr.as_deref().unwrap_or("<unknown>"),
            pid = std::process::id(),
            "brewdbd started"
        );
        self.serve_protocol(protocol_name, listener)
    }
}

pub fn bootstrap() -> Result<BrewDbServer, BrewDbServerError> {
    BrewDbServer::from_system_config(bootstrap_system_config()?)
}

pub fn load_system_config_file(path: impl AsRef<Path>) -> Result<ConfigSet, BrewDbServerError> {
    let loader = SystemConfigLoader::for_global_registry()?;
    Ok(loader.load_toml_file(path)?)
}

pub fn bootstrap_system_config() -> Result<ConfigSet, BrewDbServerError> {
    let registry = global_config_registry()?;
    let warehouse = brewdb_temp_root().join(format!("brewdb-warehouse-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&warehouse).map_err(|error| {
        BrewDbServerError::Common(CommonError::InvalidConfiguration {
            field: "brewdb.catalog.paimon.warehouse".to_owned(),
            reason: error.to_string(),
        })
    })?;
    let mut config = registry.materialize_defaults();
    config.apply_patch_with_registry(
        &registry,
        &brewdb_common::config::ConfigPatch::new(brewdb_common::config::ConfigScope::System)
            .with_entry(
                "brewdb.catalog.paimon.warehouse",
                warehouse.to_string_lossy().as_ref(),
            ),
    )?;
    Ok(config)
}

fn brewdb_temp_root() -> PathBuf {
    PathBuf::from("/tmp")
}

pub fn init_logging() -> Result<(), BrewDbServerError> {
    brewdb_common::logging::init_logging(&brewdb_common::logging::LoggingConfig::default())?;
    Ok(())
}

pub fn init_logging_from_config(
    config: &ConfigSet,
) -> Result<brewdb_common::logging::LoggingConfig, BrewDbServerError> {
    let logging_config = brewdb_common::logging::LoggingConfig::from_config_set(config)?;
    brewdb_common::logging::init_logging(&logging_config)?;
    Ok(logging_config)
}

impl SqlRequestHandler for BrewDbServer {
    fn execute(&self, request: &SqlRequest) -> Result<SqlExecutionResult, FrontendError> {
        let started_at = Instant::now();
        let query_context = &request.query_context;
        let _guard = query_context.span().entered();
        let handle = self.execute_client_request(request).map_err(|error| {
            error!(
                target: "brewdb.frontend",
                error_code = error.error_code().as_str(),
                error = %error,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "frontend query failed"
            );
            FrontendError::QueryExecutionFailed {
                error_code: Some(error.error_code()),
                reason: error.to_string(),
            }
        })?;
        let mut batches = Vec::new();
        while let Some(batch) = handle.output.next_result().map_err(|error| {
            error!(
                target: "brewdb.frontend",
                error = %error,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "frontend query result stream failed"
            );
            FrontendError::QueryExecutionFailed {
                error_code: None,
                reason: error.to_string(),
            }
        })? {
            batches.push(batch);
        }

        let response = if handle.returns_rows {
            let fields = batches
                .first()
                .map(|batch| {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| ResultField::new(field.name(), field.data_type().to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let row_count = batches.iter().map(|batch| batch.num_rows() as u64).sum();
            FrontendResponse::new(QueryResultOutput::query(
                handle.command_tag,
                row_count,
                fields,
            ))
        } else {
            FrontendResponse::new(QueryResultOutput::command(handle.command_tag))
        };

        info!(
            target: "brewdb.frontend",
            result_kind = ?response.result.kind,
            command_tag = response.result.command_tag.as_str(),
            row_count = response.result.row_count,
            batch_count = batches.len(),
            field_count = response.result.fields.len(),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "frontend query completed"
        );

        Ok(SqlExecutionResult { response, batches })
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use brewdb_catalog::{
        CatalogConfig, CatalogEntry, CatalogMode, CatalogPath, CatalogService,
        CatalogStoreBackendKind, CreateDatabaseRequest, CreateTableRequest, StorageKind,
        open_catalog_store,
    };
    use brewdb_common::config::{ConfigPatch, ConfigScope, ConfigSet, global_config_registry};
    use brewdb_common::test_util::{TestDir, TestFile};
    use brewdb_common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use brewdb_execution::runtime::QueryCoordinator;
    use brewdb_frontend::{
        ClientCapabilities, ClientDefaults, ClientIdentity, ClientSessionContext,
        DEFAULT_DATABASE_NAME, MANAGED_PAIMON_CATALOG_NAME, OpenedClientSession, PgWireCodec,
        QueryResultKind, SqlRequestHandler,
    };
    use brewdb_storage::{memory::MemoryTableEngine, open_storage_engine};
    use uuid::Uuid;

    use super::BrewDbServer;

    fn build_test_server() -> BrewDbServer {
        let warehouse = TestDir::new("brewdbd-e2e");
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry("brewdb.catalog.store.backend", "memory")
                    .with_entry(
                        "brewdb.catalog.paimon.warehouse",
                        warehouse.path().to_string_lossy().as_ref(),
                    ),
            )
            .unwrap();
        let catalog_service = CatalogService::with_config(
            open_catalog_store(&CatalogConfig {
                store_backend: CatalogStoreBackendKind::Memory,
                paimon_warehouse: warehouse.path().to_string_lossy().into_owned(),
                rocksdb_datadir: String::new(),
                rocksdb_root: String::new(),
            }),
            config,
        );
        catalog_service
            .create_catalog(CatalogEntry::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
                CatalogMode::Managed,
                StorageKind::Paimon,
            ))
            .unwrap();
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        let table = catalog
            .create_table(CreateTableRequest::new(
                "sales",
                "orders",
                TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        let storage = open_storage_engine().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        storage.register_table_engine(
            &table,
            Arc::new(
                MemoryTableEngine::try_new(
                    &table,
                    vec![vec![RecordBatch::try_new(schema, vec![values]).unwrap()]],
                )
                .unwrap(),
            ),
        );

        BrewDbServer::with_catalog_service(catalog_service, QueryCoordinator::with_storage(storage))
    }

    #[test]
    fn server_wires_frontend_sql_driver_runtime_and_storage() {
        let server = build_test_server();
        let session = OpenedClientSession {
            context: brewdb_frontend::ClientContext {
                session: ClientSessionContext::new(
                    Uuid::new_v4(),
                    ClientIdentity::new("brew").with_database("sales"),
                ),
                connection: None,
                defaults: ClientDefaults::default()
                    .with_catalog("prod")
                    .with_database("sales"),
                identity: ClientIdentity::new("brew").with_database("sales"),
                capabilities: ClientCapabilities::default(),
                settings: ConfigSet::new(),
            },
        };
        let request = server
            .frontend()
            .build_request(&session, Uuid::new_v4(), "select count(id) from orders")
            .unwrap();
        let handle = server.execute_client_request(&request).unwrap();
        let batch = handle.output.next_result().unwrap().unwrap();
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 3);
    }

    #[test]
    fn server_returns_command_result_for_ddl() {
        let server = build_test_server();
        let session = OpenedClientSession {
            context: brewdb_frontend::ClientContext {
                session: ClientSessionContext::new(
                    Uuid::new_v4(),
                    ClientIdentity::new("brew").with_database("sales"),
                ),
                connection: None,
                defaults: ClientDefaults::default()
                    .with_catalog("prod")
                    .with_database("sales"),
                identity: ClientIdentity::new("brew").with_database("sales"),
                capabilities: ClientCapabilities::default(),
                settings: ConfigSet::new(),
            },
        };
        let request = server
            .frontend()
            .build_request(&session, Uuid::new_v4(), "create database analytics")
            .unwrap();

        let result = server.execute(&request).unwrap();

        assert_eq!(result.response.result.kind, QueryResultKind::Command);
        assert_eq!(
            result.response.result.command_tag.as_str(),
            "CREATE DATABASE"
        );
        assert!(result.batches.is_empty());
    }

    #[test]
    fn server_bootstraps_default_database_for_client_sessions() {
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        let warehouse = TestDir::with_prefix("brewdbd-default-db");
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System).with_entry(
                    "brewdb.catalog.paimon.warehouse",
                    warehouse.path().to_string_lossy().as_ref(),
                ),
            )
            .unwrap();
        let server = super::BrewDbServer::from_system_config(config).unwrap();
        let session = OpenedClientSession {
            context: brewdb_frontend::ClientContext {
                session: ClientSessionContext::new(
                    Uuid::new_v4(),
                    ClientIdentity::new("brew").with_database(DEFAULT_DATABASE_NAME),
                ),
                connection: None,
                defaults: ClientDefaults::default()
                    .with_catalog(MANAGED_PAIMON_CATALOG_NAME)
                    .with_database(DEFAULT_DATABASE_NAME),
                identity: ClientIdentity::new("brew").with_database(DEFAULT_DATABASE_NAME),
                capabilities: ClientCapabilities::default(),
                settings: ConfigSet::new(),
            },
        };
        let request = server
            .frontend()
            .build_request(
                &session,
                Uuid::new_v4(),
                "create table t1 (id int not null)",
            )
            .unwrap();

        let result = server.execute(&request).unwrap();

        assert_eq!(result.response.result.kind, QueryResultKind::Command);
        assert_eq!(result.response.result.command_tag.as_str(), "CREATE TABLE");
    }

    #[test]
    fn server_inserts_values_into_managed_paimon_table() {
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        let warehouse = TestDir::with_prefix("brewdbd-insert");
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System).with_entry(
                    "brewdb.catalog.paimon.warehouse",
                    warehouse.path().to_string_lossy().as_ref(),
                ),
            )
            .unwrap();
        let server = super::BrewDbServer::from_system_config(config).unwrap();
        let session = OpenedClientSession {
            context: brewdb_frontend::ClientContext {
                session: ClientSessionContext::new(
                    Uuid::new_v4(),
                    ClientIdentity::new("brew").with_database(DEFAULT_DATABASE_NAME),
                ),
                connection: None,
                defaults: ClientDefaults::default()
                    .with_catalog(MANAGED_PAIMON_CATALOG_NAME)
                    .with_database(DEFAULT_DATABASE_NAME),
                identity: ClientIdentity::new("brew").with_database(DEFAULT_DATABASE_NAME),
                capabilities: ClientCapabilities::default(),
                settings: ConfigSet::new(),
            },
        };

        for sql in [
            "create table t_insert (id int not null)",
            "insert into t_insert values (1), (2)",
        ] {
            let request = server
                .frontend()
                .build_request(&session, Uuid::new_v4(), sql)
                .unwrap();
            let result = server.execute(&request).unwrap();
            assert_eq!(result.response.result.kind, QueryResultKind::Command);
        }

        let request = server
            .frontend()
            .build_request(&session, Uuid::new_v4(), "select count(id) from t_insert")
            .unwrap();
        let result = server.execute(&request).unwrap();
        let count = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 2);
    }

    #[test]
    fn server_rejects_schema_less_create_table() {
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        let warehouse = TestDir::with_prefix("brewdbd-empty-schema");
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System).with_entry(
                    "brewdb.catalog.paimon.warehouse",
                    warehouse.path().to_string_lossy().as_ref(),
                ),
            )
            .unwrap();
        let server = super::BrewDbServer::from_system_config(config).unwrap();
        let session = OpenedClientSession {
            context: brewdb_frontend::ClientContext {
                session: ClientSessionContext::new(
                    Uuid::new_v4(),
                    ClientIdentity::new("brew").with_database(DEFAULT_DATABASE_NAME),
                ),
                connection: None,
                defaults: ClientDefaults::default()
                    .with_catalog(MANAGED_PAIMON_CATALOG_NAME)
                    .with_database(DEFAULT_DATABASE_NAME),
                identity: ClientIdentity::new("brew").with_database(DEFAULT_DATABASE_NAME),
                capabilities: ClientCapabilities::default(),
                settings: ConfigSet::new(),
            },
        };
        let request = server
            .frontend()
            .build_request(&session, Uuid::new_v4(), "create table t1")
            .unwrap();

        let error = match server.execute(&request) {
            Ok(_) => panic!("expected create table without schema to fail"),
            Err(error) => error,
        };

        assert_eq!(
            error.to_string(),
            "query execution failed: invalid planner input: CREATE TABLE must define at least one column"
        );
    }

    #[test]
    fn server_serves_pgwire_query_through_catalog_planner_runtime_and_storage() {
        let server = Arc::new(build_test_server().with_default_catalog("prod"));
        let (server_stream, mut client_stream) = std::os::unix::net::UnixStream::pair().unwrap();
        let server_for_connection = Arc::clone(&server);
        let connection = std::thread::spawn(move || {
            PgWireCodec
                .serve_connection_io(
                    server_stream,
                    server_for_connection.frontend().clone(),
                    server_for_connection.client_defaults.clone(),
                    server_for_connection as Arc<dyn SqlRequestHandler>,
                )
                .unwrap();
        });

        let startup_payload = [
            &196_608_i32.to_be_bytes()[..],
            b"user\0brew\0database\0sales\0\0",
        ]
        .concat();
        write_startup(&mut client_stream, &startup_payload);
        assert_eq!(read_frame(&mut client_stream).0, b'R');
        assert_eq!(read_frame(&mut client_stream).0, b'Z');

        write_message(
            &mut client_stream,
            b'Q',
            b"select count(id) from prod.sales.orders\0",
        );
        let first_response = read_frame(&mut client_stream);
        assert_eq!(first_response.0, b'T', "{first_response:?}");
        let data_row = read_frame(&mut client_stream);
        assert_eq!(data_row.0, b'D');
        assert_eq!(data_row.1[0..2], [0, 1]);
        assert_eq!(data_row.1[6..7], [b'3']);
        assert_eq!(read_frame(&mut client_stream).0, b'C');
        assert_eq!(read_frame(&mut client_stream).0, b'Z');

        write_message(&mut client_stream, b'X', &[]);
        drop(client_stream);
        connection.join().unwrap();
    }

    fn write_startup(stream: &mut impl Write, payload: &[u8]) {
        let length = (payload.len() + 4) as i32;
        stream.write_all(&length.to_be_bytes()).unwrap();
        stream.write_all(payload).unwrap();
    }

    fn write_message(stream: &mut impl Write, message_type: u8, payload: &[u8]) {
        let length = (payload.len() + 4) as i32;
        stream.write_all(&[message_type]).unwrap();
        stream.write_all(&length.to_be_bytes()).unwrap();
        stream.write_all(payload).unwrap();
    }

    fn read_frame(stream: &mut impl Read) -> (u8, Vec<u8>) {
        let mut message_type = [0; 1];
        stream.read_exact(&mut message_type).unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut payload = vec![0; i32::from_be_bytes(length) as usize - 4];
        stream.read_exact(&mut payload).unwrap();
        (message_type[0], payload)
    }

    #[test]
    fn server_can_be_constructed_from_system_config() {
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry("brewdb.catalog.store.backend", "memory"),
            )
            .unwrap();
        let warehouse = TestDir::with_prefix("brewdbd-test");
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System).with_entry(
                    "brewdb.catalog.paimon.warehouse",
                    warehouse.path().to_string_lossy().as_ref(),
                ),
            )
            .unwrap();

        let server = super::BrewDbServer::from_system_config(config).unwrap();
        let session = OpenedClientSession {
            context: brewdb_frontend::ClientContext {
                session: ClientSessionContext::new(
                    Uuid::new_v4(),
                    ClientIdentity::new("brew").with_database("sales"),
                ),
                connection: None,
                defaults: ClientDefaults::default().with_catalog(MANAGED_PAIMON_CATALOG_NAME),
                identity: ClientIdentity::new("brew").with_database("sales"),
                capabilities: ClientCapabilities::default(),
                settings: ConfigSet::new(),
            },
        };
        let request = server
            .frontend()
            .build_request(&session, Uuid::new_v4(), "create database analytics")
            .unwrap();
        let result = server.execute(&request).unwrap();
        assert_eq!(result.response.result.kind, QueryResultKind::Command);
    }

    #[test]
    fn server_can_be_constructed_from_config_file() {
        let warehouse = TestDir::with_prefix("brewdbd-config");
        let path = TestFile::new("brewdbd-config", "toml");
        std::fs::write(
            path.path(),
            format!(
                "brewdb.catalog.store.backend = \"memory\"\nbrewdb.catalog.paimon.warehouse = \"{}\"\n",
                warehouse.path().to_string_lossy()
            ),
        )
        .unwrap();

        let server = super::BrewDbServer::from_config_file(path.path()).unwrap();
        let session = OpenedClientSession {
            context: brewdb_frontend::ClientContext {
                session: ClientSessionContext::new(
                    Uuid::new_v4(),
                    ClientIdentity::new("brew").with_database("sales"),
                ),
                connection: None,
                defaults: ClientDefaults::default().with_catalog(MANAGED_PAIMON_CATALOG_NAME),
                identity: ClientIdentity::new("brew").with_database("sales"),
                capabilities: ClientCapabilities::default(),
                settings: ConfigSet::new(),
            },
        };
        let request = server
            .frontend()
            .build_request(&session, Uuid::new_v4(), "create database analytics")
            .unwrap();
        let result = server.execute(&request).unwrap();
        assert_eq!(result.response.result.kind, QueryResultKind::Command);
    }

    #[test]
    fn logging_config_can_be_loaded_from_server_config_file() {
        let warehouse = TestDir::with_prefix("brewdbd-log-config");
        let log_path = TestFile::new("brewdbd-log", "log");
        let path = TestFile::new("brewdbd-log-config", "toml");
        std::fs::write(
            path.path(),
            format!(
                r#"
brewdb.catalog.store.backend = "memory"
brewdb.catalog.paimon.warehouse = "{}"
brewdb.logging.level = "debug"
brewdb.logging.filter = "info,datafusion=warn,paimon=debug"
brewdb.logging.path = "{}"
brewdb.logging.rolling_policy = "hourly"
brewdb.logging.format = "json"
"#,
                warehouse.path().to_string_lossy(),
                log_path.path().to_string_lossy()
            ),
        )
        .unwrap();

        let config = super::load_system_config_file(path.path()).unwrap();
        let logging = brewdb_common::logging::LoggingConfig::from_config_set(&config).unwrap();

        assert_eq!(logging.level, brewdb_common::logging::LogLevel::Debug);
        assert_eq!(
            logging.path.as_deref(),
            Some(log_path.path().to_string_lossy().as_ref())
        );
        assert_eq!(
            logging.rolling_policy,
            brewdb_common::logging::RollingPolicy::Hourly
        );
        assert_eq!(logging.filter, "info,datafusion=warn,paimon=debug");
    }
}
