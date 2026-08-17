use std::fs;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn sqllogic_fixtures_pass_through_shared_server_and_client() {
    let mut harness = ServerHarness::start();
    harness.run_fixture("sqllogic/basic.test");
}

struct ServerHarness {
    _sandbox: TestDir,
    config_path: PathBuf,
    port: u16,
    server: Child,
    client_bin: PathBuf,
}

impl ServerHarness {
    fn start() -> Self {
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

    fn run_fixture(&mut self, path: impl AsRef<Path>) {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path);
        let source = fs::read_to_string(&fixture_path).unwrap_or_else(|error| {
            panic!(
                "fixture {} must be readable: {error}",
                fixture_path.display()
            )
        });
        let cases = parse_fixture(&source).unwrap_or_else(|error| {
            panic!("fixture {} is invalid: {error}", fixture_path.display())
        });

        for case in cases {
            match case {
                FixtureCase::Statement { sql } => {
                    self.run_client(&sql)
                        .unwrap_or_else(|error| panic!("statement failed:\n{sql}\n\n{error}"));
                }
                FixtureCase::Query { sql, expected } => {
                    let stdout = self
                        .run_client(&sql)
                        .unwrap_or_else(|error| panic!("query failed:\n{sql}\n\n{error}"));
                    let actual = parse_client_rows(&stdout);
                    assert_eq!(actual, expected, "query output mismatch:\n{sql}");
                }
            }
        }
    }

    fn run_client(&self, sql: &str) -> Result<String, String> {
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
            .map_err(|error| format!("failed to launch brewdb client: {error}"))?;
        if output.status.success() {
            return String::from_utf8(output.stdout)
                .map_err(|error| format!("client stdout was not utf8: {error}"));
        }
        Err(format!(
            "client exited with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

impl Drop for ServerHarness {
    fn drop(&mut self) {
        let _ = self.server.kill();
        let _ = self.server.wait();
        let _ = fs::remove_file(&self.config_path);
    }
}

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("brewdb-sqllogic-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
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

#[derive(Debug, PartialEq, Eq)]
enum FixtureCase {
    Statement { sql: String },
    Query { sql: String, expected: Vec<String> },
}

fn parse_fixture(source: &str) -> Result<Vec<FixtureCase>, String> {
    source
        .split("\n\n")
        .map(str::trim)
        .filter(|block| !block.is_empty())
        .filter(|block| !block.starts_with('#'))
        .map(parse_block)
        .collect()
}

fn parse_block(block: &str) -> Result<FixtureCase, String> {
    let mut lines = block.lines();
    let directive = lines
        .next()
        .ok_or_else(|| "fixture block is empty".to_owned())?
        .trim();
    let body = lines.collect::<Vec<_>>().join("\n");
    match directive {
        "statement ok" => Ok(FixtureCase::Statement {
            sql: body.trim().to_owned(),
        }),
        "query" => {
            let (sql, expected) = body
                .split_once("----")
                .ok_or_else(|| "query block is missing ---- separator".to_owned())?;
            Ok(FixtureCase::Query {
                sql: sql.trim().to_owned(),
                expected: expected
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(ToOwned::to_owned)
                    .collect(),
            })
        }
        other => Err(format!("unsupported fixture directive: {other}")),
    }
}

fn parse_client_rows(stdout: &str) -> Vec<String> {
    let table_rows = stdout
        .lines()
        .filter_map(parse_table_row)
        .collect::<Vec<_>>();
    table_rows.into_iter().skip(1).collect()
}

fn parse_table_row(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if !trimmed.starts_with('|') || !trimmed.ends_with('|') {
        return None;
    }
    Some(
        trimmed
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect::<Vec<_>>()
            .join(" "),
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
    use super::{FixtureCase, parse_client_rows, parse_fixture};

    #[test]
    fn parser_reads_statement_and_query_blocks() {
        let cases = parse_fixture(
            r#"
statement ok
create table orders (id int);

query
select 1;
----
1
"#,
        )
        .unwrap();

        assert_eq!(
            cases,
            vec![
                FixtureCase::Statement {
                    sql: "create table orders (id int);".to_owned()
                },
                FixtureCase::Query {
                    sql: "select 1;".to_owned(),
                    expected: vec!["1".to_owned()]
                }
            ]
        );
    }

    #[test]
    fn client_table_output_rows_are_normalized() {
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

        assert_eq!(rows, vec!["1 alice".to_owned(), "2 bob".to_owned()]);
    }
}
