use std::fmt;
use std::fs;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use brewdb_bin::client::PgWireSession;
use brewdb_common::test_util::{TestDir, temp_root};
use sqllogictest::{DBOutput, DefaultColumnType};

pub struct ServerHarness {
    _sandbox: TestDir,
    config_path: PathBuf,
    port: u16,
    server: Child,
}

impl ServerHarness {
    pub fn start() -> Self {
        build_brewdb_bins();
        let target_dir = target_debug_dir();
        let server_bin = binary_path(&target_dir, "brewdbd");

        let sandbox = TestDir::new("brewdb-sqllogic");
        let port = reserve_port();
        let config_path = sandbox.path().join("brewdb.toml");
        fs::write(
            &config_path,
            format!(
                r#"
brewdb.frontend.pgwire.listen_addr = "127.0.0.1:{port}"
brewdb.catalog.store.backend = "memory"
brewdb.catalog.paimon.warehouse = "{}"
"#,
                sandbox.path().join("warehouse").to_string_lossy()
            ),
        )
        .unwrap();

        let server = Command::new(server_bin)
            .arg("--config")
            .arg(&config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        wait_for_server(port);

        Self {
            _sandbox: sandbox,
            config_path,
            port,
            server,
        }
    }

    pub fn run_fixture(&self, path: impl AsRef<Path>) -> Result<(), sqllogictest::TestError> {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path);
        let client = self.client();
        let mut runner = sqllogictest::Runner::new(move || {
            let client = client.clone();
            async move { Ok::<_, ClientError>(client) }
        });
        runner.set_var("TEST_DATA_DIR".to_owned(), test_data_dir());
        runner.run_file(&fixture_path)
    }

    fn client(&self) -> BrewDbClient {
        BrewDbClient { port: self.port }
    }
}

impl Drop for ServerHarness {
    fn drop(&mut self) {
        let _ = self.server.kill();
        let _ = self.server.wait();
        let _ = fs::remove_file(&self.config_path);
    }
}

#[derive(Clone)]
struct BrewDbClient {
    port: u16,
}

impl sqllogictest::DB for BrewDbClient {
    type Error = ClientError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let result = self.run_client(sql)?;
        let rows = result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|value| value.unwrap_or_else(|| "NULL".to_owned()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if !result.headers.is_empty() {
            let types = rows
                .first()
                .map(|row| vec![DefaultColumnType::Any; row.len()])
                .unwrap_or_else(|| vec![DefaultColumnType::Any; result.headers.len()]);
            return Ok(DBOutput::Rows { types, rows });
        }
        Ok(DBOutput::StatementComplete(0))
    }
}

impl BrewDbClient {
    fn run_client(&self, sql: &str) -> Result<brewdb_bin::client::QueryResult, ClientError> {
        let stream = TcpStream::connect(("127.0.0.1", self.port))
            .map_err(|error| ClientError(format!("failed to connect to brewdbd: {error}")))?;
        stream.set_nodelay(true).ok();
        let mut session = PgWireSession::new(stream);
        let user = std::env::var("USER").unwrap_or_else(|_| "brew".to_owned());
        session
            .startup(&user, Some("brewdb"))
            .map_err(|error| ClientError(error.to_string()))?;
        let result = session
            .execute(sql)
            .map_err(|error| ClientError(error.to_string()))?;
        session
            .terminate()
            .map_err(|error| ClientError(error.to_string()))?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
struct ClientError(String);

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ClientError {}

pub fn test_sandbox_root() -> PathBuf {
    temp_root()
}

pub fn test_data_dir() -> String {
    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    data_dir.to_string_lossy().into_owned()
}

fn build_brewdb_bins() {
    let status = Command::new(env!("CARGO"))
        .arg("build")
        .arg("-p")
        .arg("brewdb-bin")
        .arg("--bin")
        .arg("brewdbd")
        .stdin(Stdio::null())
        .status()
        .expect("cargo build for brewdbd must start");
    assert!(
        status.success(),
        "cargo build -p brewdb-bin --bin brewdbd failed"
    );
}

fn target_debug_dir() -> PathBuf {
    let current_exe = std::env::current_exe().unwrap();
    current_exe
        .parent()
        .and_then(Path::parent)
        .expect("integration test must run from target/debug/deps")
        .to_path_buf()
}

fn binary_path(target_dir: &Path, name: &str) -> PathBuf {
    let suffix = std::env::consts::EXE_SUFFIX;
    let path = target_dir.join(format!("{name}{suffix}"));
    assert!(
        path.exists(),
        "expected binary `{}` to exist after cargo build",
        path.display()
    );
    path
}

fn reserve_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_server(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("brewdbd did not start listening on port {port}");
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{ClientError, test_data_dir};

    #[test]
    fn sqllogictest_error_regex_matches_client_error_code() {
        let error = ClientError("brewdb failed: BREWDB_PLANNER_SCHEMA_ERROR: bad".to_owned());
        let record = sqllogictest::parse::<sqllogictest::DefaultColumnType>(
            r#"
statement error BREWDB_PLANNER_SCHEMA_ERROR:
select bad;
"#,
        )
        .unwrap()
        .pop()
        .unwrap();

        let mut runner = sqllogictest::Runner::new(move || {
            let error = error.clone();
            async move { Ok::<_, ClientError>(AlwaysFails(error)) }
        });

        runner.run(record).unwrap();
    }

    #[test]
    fn test_data_dir_points_to_fixture_data_directory() {
        let data_dir = test_data_dir();

        assert!(data_dir.contains("crates/brewdb-sqllogictests/data"));
    }

    #[test]
    fn test_dir_uses_tmp_root_for_inspection() {
        let dir = super::TestDir::new("brewdb-sqllogic");

        assert_eq!(dir.path().parent().unwrap(), Path::new("/tmp"));
        assert!(
            dir.path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("brewdb-sqllogic-")
        );
    }

    struct AlwaysFails(ClientError);

    impl sqllogictest::DB for AlwaysFails {
        type Error = ClientError;
        type ColumnType = sqllogictest::DefaultColumnType;

        fn run(
            &mut self,
            _sql: &str,
        ) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
            Err(self.0.clone())
        }
    }
}
