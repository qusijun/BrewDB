//! BrewDB SQL parse and bind boundary.

pub mod binder;
pub mod errors;
pub mod ingress;
pub mod parser;
pub mod statement;

pub use binder::{SqlBinder, context::StatementBindingContext};
pub use errors::SqlError;
pub use ingress::{SqlClientCapabilities, SqlIngressRequest, SqlRequestContext, SqlSessionContext};
pub use parser::SqlParser;
pub use statement::{
    BoundAlterStatement, BoundAlterTableOperation, BoundAlterTableStatement, BoundBeginStatement,
    BoundCommitStatement, BoundCreateDatabaseStatement, BoundCreateStatement,
    BoundCreateTableStatement, BoundDeleteStatement, BoundDropDatabaseStatement,
    BoundDropStatement, BoundDropTableStatement, BoundExplainStatement, BoundInsertStatement,
    BoundMergeStatement, BoundPlanStatement, BoundQueryStatement, BoundRollbackStatement,
    BoundSessionContext, BoundSetStatement, BoundSetVariableStatement, BoundShowStatement,
    BoundStatement, BoundTransactionStatement, BoundUpdateStatement, BoundUseDatabaseStatement,
    ExplainKind, ParsedStatement, ParsedStatementKind, SetScope, TransactionMode,
};

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use brewdb_catalog::{CatalogConfig, CatalogStoreBackendKind};
    use brewdb_catalog::{
        CatalogEntry, CatalogMode, CatalogPath, CatalogService, CreateDatabaseRequest,
        CreateTableRequest, LakeFormatKind, open_catalog_store,
    };
    use brewdb_common::config::{ConfigPatch, ConfigScope, global_config_registry};
    use brewdb_common::defaults::MANAGED_PAIMON_CATALOG_NAME;
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};
    use uuid::Uuid;

    use crate::binder::context::StatementBindingContext;
    use crate::statement::{BoundPlanStatement, BoundSetStatement, BoundStatement};
    use crate::{SqlBinder, SqlParser};

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).expect("test directory must be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn catalog_service() -> CatalogService {
        let store = open_catalog_store(&CatalogConfig {
            store_backend: CatalogStoreBackendKind::Memory,
            paimon_warehouse: String::new(),
        });
        let warehouse = TestDir::new("brewdb-sql-tests");
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
        let service = CatalogService::with_config(store, config);
        let entry = CatalogEntry::new(
            Uuid::new_v4(),
            CatalogPath::new(MANAGED_PAIMON_CATALOG_NAME).unwrap(),
            CatalogMode::Managed,
            LakeFormatKind::Paimon,
        );
        service.create_catalog(entry).unwrap();
        let catalog = service.open_catalog(MANAGED_PAIMON_CATALOG_NAME).unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("brewdb"))
            .unwrap();
        catalog
            .create_table(
                CreateTableRequest::new(
                    "brewdb",
                    "orders",
                    TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();
        catalog
            .create_table(
                CreateTableRequest::new(
                    "brewdb",
                    "customers",
                    TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();
        std::mem::forget(warehouse);
        service
    }

    fn bind(sql: &str) -> BoundStatement {
        let service = catalog_service();
        let parser = SqlParser;
        let binder = SqlBinder;
        let parsed = parser.parse_one(sql).unwrap();
        binder
            .bind(
                parsed,
                &StatementBindingContext {
                    session: &crate::SqlSessionContext {
                        session_id: Uuid::nil(),
                        user_name: "brew".to_owned(),
                        catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                        database_name: Some("brewdb".to_owned()),
                    },
                    request: &crate::SqlRequestContext {
                        request_id: Uuid::nil(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap()
    }

    #[test]
    fn parser_and_binder_turn_select_into_plan_statement() {
        let bound = bind("select * from orders");

        match bound {
            BoundStatement::Plan(BoundPlanStatement::Query(statement)) => {
                assert_eq!(statement.session.catalog_name, MANAGED_PAIMON_CATALOG_NAME);
                assert_eq!(statement.statement_text, "select * from orders");
                assert_eq!(statement.tables.len(), 1);
                assert_eq!(statement.tables[0].path.table(), "orders");
            }
            other => panic!("expected query plan statement, got {other:?}"),
        }
    }

    #[test]
    fn binder_turns_insert_select_into_bound_source_tables() {
        let bound = bind("insert into orders select * from customers");

        match bound {
            BoundStatement::Plan(BoundPlanStatement::Insert(statement)) => {
                assert_eq!(statement.target_table.path.table(), "orders");
                assert_eq!(statement.source_tables.len(), 1);
                assert_eq!(statement.source_tables[0].path.table(), "customers");
            }
            other => panic!("expected insert plan statement, got {other:?}"),
        }
    }

    #[test]
    fn binder_turns_set_into_set_statement() {
        let bound = bind("set work_mem = '128MB'");

        match bound {
            BoundStatement::Set(BoundSetStatement::Variable(statement)) => {
                assert_eq!(statement.key, "work_mem");
                assert_eq!(statement.value, "128MB");
            }
            other => panic!("expected set statement, got {other:?}"),
        }
    }

    #[test]
    fn binder_turns_create_table_into_bound_create_statement() {
        let bound =
            bind("create table t1 (id int not null, name text) distributed by (id) into 4 buckets");

        match bound {
            BoundStatement::Create(crate::BoundCreateStatement::Table(statement)) => {
                assert_eq!(statement.catalog_name, MANAGED_PAIMON_CATALOG_NAME);
                assert_eq!(statement.database_name, "brewdb");
                assert_eq!(statement.table_name, "t1");
                assert_eq!(statement.table_schema.fields.len(), 2);
                assert!(!statement.table_schema.fields[0].nullable);
                assert_eq!(statement.primary_keys, Vec::<String>::new());
                assert_eq!(
                    statement
                        .table_options
                        .get("bucket-key")
                        .map(String::as_str),
                    Some("id")
                );
                assert_eq!(
                    statement.table_options.get("bucket").map(String::as_str),
                    Some("4")
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn parser_represents_create_table_distribution_in_sqlparser_ast() {
        let parsed = SqlParser
            .parse_one("create table t1 (id int not null, primary key (id)) distributed by hash(id) into 4 buckets")
            .unwrap();

        match parsed.ast {
            brewdb_sql_parser::ast::Statement::CreateTable(statement) => {
                assert_eq!(statement.name.to_string(), "t1");
                assert_eq!(statement.columns.len(), 1);
                let distribution = statement.distributed_by.unwrap();
                assert_eq!(
                    distribution.function.map(|ident| ident.value),
                    Some("hash".to_owned())
                );
                assert_eq!(
                    distribution
                        .columns
                        .into_iter()
                        .map(|ident| ident.value)
                        .collect::<Vec<_>>(),
                    vec!["id"]
                );
                assert_eq!(distribution.buckets, Some(4));
            }
            other => panic!("expected create table AST, got {other:?}"),
        }
    }

    #[test]
    fn binder_turns_distributed_by_function_into_bucket_options() {
        let bound = bind("create table t1 (id int not null) distributed by mod(id) into 8 buckets");

        match bound {
            BoundStatement::Create(crate::BoundCreateStatement::Table(statement)) => {
                assert_eq!(
                    statement
                        .table_options
                        .get("bucket-function.type")
                        .map(String::as_str),
                    Some("mod")
                );
                assert_eq!(
                    statement.table_options.get("bucket").map(String::as_str),
                    Some("8")
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn binder_accepts_paimon_bucket_functions() {
        let bound =
            bind("create table t1 (id int not null) distributed by hive(id) into 8 buckets");
        match bound {
            BoundStatement::Create(crate::BoundCreateStatement::Table(statement)) => {
                assert_eq!(
                    statement
                        .table_options
                        .get("bucket-function.type")
                        .map(String::as_str),
                    Some("hive")
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }

        let bound =
            bind("create table t1 (id int not null) distributed by hash(id) into 8 buckets");
        match bound {
            BoundStatement::Create(crate::BoundCreateStatement::Table(statement)) => {
                assert_eq!(
                    statement
                        .table_options
                        .get("bucket-function.type")
                        .map(String::as_str),
                    None
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }

        let bound =
            bind("create table t1 (id int not null) distributed by default(id) into 8 buckets");
        match bound {
            BoundStatement::Create(crate::BoundCreateStatement::Table(statement)) => {
                assert_eq!(
                    statement
                        .table_options
                        .get("bucket-function.type")
                        .map(String::as_str),
                    None
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn binder_rejects_unsupported_bucket_function() {
        let service = catalog_service();
        let parser = SqlParser;
        let binder = SqlBinder;
        let parsed = parser
            .parse_one(
                "create table t1 (id int not null) distributed by murmur3(id) into 8 buckets",
            )
            .unwrap();
        let error = binder
            .bind(
                parsed,
                &StatementBindingContext {
                    session: &crate::SqlSessionContext {
                        session_id: Uuid::nil(),
                        user_name: "brew".to_owned(),
                        catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                        database_name: Some("brewdb".to_owned()),
                    },
                    request: &crate::SqlRequestContext {
                        request_id: Uuid::nil(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid sql request: unsupported Paimon bucket function `murmur3`; supported functions are default, hash, mod, hive"
        );
    }

    #[test]
    fn binder_accepts_distribution_with_bucket_count_only() {
        let bound = bind("create table t1 (id int not null) distributed into 4 buckets");

        match bound {
            BoundStatement::Create(crate::BoundCreateStatement::Table(statement)) => {
                assert_eq!(
                    statement.table_options.get("bucket").map(String::as_str),
                    Some("4")
                );
                assert_eq!(
                    statement
                        .table_options
                        .get("bucket-key")
                        .map(String::as_str),
                    None
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn parser_represents_distribution_with_bucket_count_only() {
        let parsed = SqlParser
            .parse_one("create table t1 (id int not null) distributed into 4 buckets")
            .unwrap();

        match parsed.ast {
            brewdb_sql_parser::ast::Statement::CreateTable(statement) => {
                let distribution = statement.distributed_by.unwrap();
                assert_eq!(distribution.function, None);
                assert!(distribution.columns.is_empty());
                assert_eq!(distribution.buckets, Some(4));
            }
            other => panic!("expected create table AST, got {other:?}"),
        }
    }

    #[test]
    fn binder_rejects_create_table_without_columns() {
        let service = catalog_service();
        let parser = SqlParser;
        let binder = SqlBinder;
        let parsed = parser.parse_one("create table t1").unwrap();
        let error = binder
            .bind(
                parsed,
                &StatementBindingContext {
                    session: &crate::SqlSessionContext {
                        session_id: Uuid::nil(),
                        user_name: "brew".to_owned(),
                        catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                        database_name: Some("brewdb".to_owned()),
                    },
                    request: &crate::SqlRequestContext {
                        request_id: Uuid::nil(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid sql request: CREATE TABLE must define at least one column"
        );
    }

    #[test]
    fn binder_turns_show_statements_into_show_contracts() {
        let bound = bind("show catalogs");
        match bound {
            BoundStatement::Show(crate::BoundShowStatement::Catalogs) => {}
            other => panic!("expected show catalogs statement, got {other:?}"),
        }
        let bound = bind("show catalog");
        match bound {
            BoundStatement::Show(crate::BoundShowStatement::Catalogs) => {}
            other => panic!("expected show catalog statement, got {other:?}"),
        }

        let bound = bind("show databases");
        match bound {
            BoundStatement::Show(crate::BoundShowStatement::Databases { catalog_name }) => {
                assert_eq!(catalog_name, MANAGED_PAIMON_CATALOG_NAME);
            }
            other => panic!("expected show databases statement, got {other:?}"),
        }
        let bound = bind("show database");
        match bound {
            BoundStatement::Show(crate::BoundShowStatement::Databases { catalog_name }) => {
                assert_eq!(catalog_name, MANAGED_PAIMON_CATALOG_NAME);
            }
            other => panic!("expected show database statement, got {other:?}"),
        }

        let bound = bind("show tables");
        match bound {
            BoundStatement::Show(crate::BoundShowStatement::Tables {
                catalog_name,
                database_name,
            }) => {
                assert_eq!(catalog_name, MANAGED_PAIMON_CATALOG_NAME);
                assert_eq!(database_name, "brewdb");
            }
            other => panic!("expected show tables statement, got {other:?}"),
        }
        let bound = bind("show table");
        match bound {
            BoundStatement::Show(crate::BoundShowStatement::Tables {
                catalog_name,
                database_name,
            }) => {
                assert_eq!(catalog_name, MANAGED_PAIMON_CATALOG_NAME);
                assert_eq!(database_name, "brewdb");
            }
            other => panic!("expected show table statement, got {other:?}"),
        }
    }
}
