use std::fmt;
use std::fs;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use sqllogictest::{DBOutput, DefaultColumnType};

pub struct ServerHarness {
    _sandbox: TestDir,
    config_path: PathBuf,
    port: u16,
    server: Child,
    client_bin: PathBuf,
}

impl ServerHarness {
    pub fn start() -> Self {
        build_brewdb_bins();
        let target_dir = target_debug_dir();
        let server_bin = binary_path(&target_dir, "brewdbd");
        let client_bin = binary_path(&target_dir, "brewdb");

        let sandbox = TestDir::new();
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
            client_bin,
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
        BrewDbClient {
            client_bin: self.client_bin.clone(),
            port: self.port,
        }
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
    client_bin: PathBuf,
    port: u16,
}

impl sqllogictest::DB for BrewDbClient {
    type Error = ClientError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let stdout = self.run_client(sql)?;
        if let Some(rows) = parse_client_rows(&stdout) {
            let types = rows
                .first()
                .map(|row| vec![DefaultColumnType::Any; row.len()])
                .unwrap_or_default();
            return Ok(DBOutput::Rows { types, rows });
        }
        Ok(DBOutput::StatementComplete(0))
    }
}

impl BrewDbClient {
    fn run_client(&self, sql: &str) -> Result<String, ClientError> {
        let output = Command::new(&self.client_bin)
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(self.port.to_string())
            .arg("--database")
            .arg("brewdb")
            .arg("--execute")
            .arg(sql)
            .output()
            .map_err(|error| ClientError(format!("failed to launch brewdb client: {error}")))?;
        if output.status.success() {
            return String::from_utf8(output.stdout)
                .map_err(|error| ClientError(format!("client stdout was not utf8: {error}")));
        }
        Err(ClientError(format!(
            "client exited with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )))
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

pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new() -> Self {
        let path = test_sandbox_root().join(format!("brewdb-sqllogic-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub fn test_sandbox_root() -> PathBuf {
    PathBuf::from("/tmp")
}

pub fn test_data_dir() -> String {
    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data");
    data_dir.to_string_lossy().into_owned()
}

fn parse_client_rows(stdout: &str) -> Option<Vec<Vec<String>>> {
    let table_rows = stdout
        .lines()
        .filter_map(parse_table_row)
        .collect::<Vec<_>>();
    if table_rows.is_empty() {
        None
    } else {
        Some(table_rows.into_iter().skip(1).collect())
    }
}

fn parse_table_row(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    if !trimmed.starts_with('|') || !trimmed.ends_with('|') {
        return None;
    }
    Some(
        trimmed
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>(),
    )
}

fn build_brewdb_bins() {
    let status = Command::new(env!("CARGO"))
        .arg("build")
        .arg("-p")
        .arg("brewdb-bin")
        .arg("--bins")
        .stdin(Stdio::null())
        .status()
        .expect("cargo build for brewdb binaries must start");
    assert!(status.success(), "cargo build -p brewdb-bin --bins failed");
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

    use super::{ClientError, parse_client_rows, test_data_dir};

    #[test]
    fn client_table_output_rows_are_converted_to_sqllogictest_rows() {
        let rows = parse_client_rows(
            r#"
+----+-------+
| id | name  |
+----+-------+
| 1  | alice |
| 2  | bob   |
+----+-------+
2 rows
SELECT
"#,
        );

        assert_eq!(
            rows,
            Some(vec![
                vec!["1".to_owned(), "alice".to_owned()],
                vec!["2".to_owned(), "bob".to_owned()]
            ])
        );
    }

    #[test]
    fn client_without_table_output_is_a_statement() {
        assert_eq!(parse_client_rows("CREATE TABLE\n"), None);
    }

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
        let dir = super::TestDir::new();

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
