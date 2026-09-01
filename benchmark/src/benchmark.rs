use std::fs;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::clickbench::clickbench_load_sql;
use crate::tpch::tpch_load_sql;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Workload {
    Tpch,
    ClickBench,
}

impl Workload {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "tpch" => Some(Self::Tpch),
            "clickbench" => Some(Self::ClickBench),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Tpch => "tpch",
            Self::ClickBench => "clickbench",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaimonFileFormat {
    Parquet,
    Vortex,
}

impl PaimonFileFormat {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "parquet" => Ok(Self::Parquet),
            "vortex" => Ok(Self::Vortex),
            _ => Err(format!(
                "unsupported file format `{value}`, expected `parquet` or `vortex`"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkRunConfig {
    pub workload: Workload,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub data_dir: Option<PathBuf>,
    pub queries_dir: Option<PathBuf>,
    pub query_file: Option<PathBuf>,
    pub iterations: usize,
    pub setup: bool,
    pub config_path: Option<PathBuf>,
    pub paimon_file_format: PaimonFileFormat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryCase {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct QueryRunResult {
    pub query_name: String,
    pub iteration: usize,
    pub elapsed: Duration,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone)]
pub struct BenchmarkReport {
    pub workload: Workload,
    pub results: Vec<QueryRunResult>,
}

pub fn run_benchmark(config: &BenchmarkRunConfig) -> io::Result<BenchmarkReport> {
    let mut executor = PgWireSqlExecutor::connect(config)?;
    run_benchmark_with_executor(config, &mut executor)
}

pub trait SqlExecutor {
    fn execute(&mut self, sql: &str) -> io::Result<SqlExecutionOutput>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlExecutionOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

pub fn run_benchmark_with_executor(
    config: &BenchmarkRunConfig,
    executor: &mut impl SqlExecutor,
) -> io::Result<BenchmarkReport> {
    if config.setup {
        if let Some(data_dir) = &config.data_dir {
            let setup_sql = match config.workload {
                Workload::Tpch => tpch_load_sql(data_dir, config.paimon_file_format),
                Workload::ClickBench => clickbench_load_sql(data_dir, config.paimon_file_format),
            };
            execute_setup_sql(executor, &setup_sql)?;
        }
    }

    let queries = load_queries(config)?;
    let mut results = Vec::new();
    for iteration in 1..=config.iterations {
        for query in &queries {
            let started = Instant::now();
            let output = executor.execute(&query.sql)?;
            results.push(QueryRunResult {
                query_name: query.name.clone(),
                iteration,
                elapsed: started.elapsed(),
                success: output.success,
                stdout: output.stdout,
                stderr: output.stderr,
            });
        }
    }

    Ok(BenchmarkReport {
        workload: config.workload.clone(),
        results,
    })
}

fn execute_setup_sql(executor: &mut impl SqlExecutor, sql: &str) -> io::Result<()> {
    for (index, statement) in split_sql_statements(sql).into_iter().enumerate() {
        let output = executor.execute(&statement)?;
        if !output.success {
            return Err(sql_execution_error(
                format!("setup statement {} failed", index + 1),
                &output,
            ));
        }
    }
    Ok(())
}

pub struct PgWireSqlExecutor {
    stream: TcpStream,
}

impl PgWireSqlExecutor {
    fn connect(config: &BenchmarkRunConfig) -> io::Result<Self> {
        let address = (config.host.as_str(), config.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::other("host resolved to no socket addresses"))?;
        let stream = TcpStream::connect(address)?;
        stream.set_nodelay(true).ok();

        let mut executor = Self { stream };
        let user = std::env::var("USER").unwrap_or_else(|_| "brew".to_owned());
        executor.startup(&user, Some(&config.database))?;
        Ok(executor)
    }

    fn startup(&mut self, user: &str, database: Option<&str>) -> io::Result<()> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&196_608_i32.to_be_bytes());
        payload.extend_from_slice(b"user\0");
        payload.extend_from_slice(user.as_bytes());
        payload.push(0);
        if let Some(database) = database {
            payload.extend_from_slice(b"database\0");
            payload.extend_from_slice(database.as_bytes());
            payload.push(0);
        }
        payload.push(0);

        let length = (payload.len() + 4) as i32;
        self.stream.write_all(&length.to_be_bytes())?;
        self.stream.write_all(&payload)?;
        self.stream.flush()?;
        self.read_until_ready()
    }

    fn read_until_ready(&mut self) -> io::Result<()> {
        loop {
            let (message_type, payload) = read_frame(&mut self.stream)?;
            match message_type {
                b'R' => {
                    if payload.len() != 4 {
                        return Err(io::Error::other("invalid authentication response"));
                    }
                    let code = i32::from_be_bytes(payload.as_slice().try_into().unwrap());
                    if code != 0 {
                        return Err(io::Error::other(format!(
                            "unsupported authentication method {code}"
                        )));
                    }
                }
                b'Z' => return Ok(()),
                b'N' => {}
                b'E' => return Err(io::Error::other(parse_error_message(&payload))),
                other => {
                    return Err(io::Error::other(format!(
                        "unexpected startup message `{}`",
                        other as char
                    )));
                }
            }
        }
    }

    fn terminate(&mut self) -> io::Result<()> {
        self.stream.write_all(b"X")?;
        self.stream.write_all(&4_i32.to_be_bytes())?;
        self.stream.flush()
    }
}

impl SqlExecutor for PgWireSqlExecutor {
    fn execute(&mut self, sql: &str) -> io::Result<SqlExecutionOutput> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.extend_from_slice(sql.as_bytes());
        payload.push(0);
        self.stream.write_all(b"Q")?;
        self.stream
            .write_all(&((payload.len() + 4) as i32).to_be_bytes())?;
        self.stream.write_all(&payload)?;
        self.stream.flush()?;

        let mut command_tag = String::new();
        let mut server_error = None;
        loop {
            let (message_type, payload) = read_frame(&mut self.stream)?;
            match message_type {
                b'C' => command_tag = parse_cstring(&payload),
                b'E' => server_error = Some(parse_error_message(&payload)),
                b'T' | b'D' | b'N' => {}
                b'Z' => break,
                other => {
                    return Err(io::Error::other(format!(
                        "unexpected query message `{}`",
                        other as char
                    )));
                }
            }
        }

        if let Some(error) = server_error {
            return Ok(SqlExecutionOutput {
                success: false,
                stdout: String::new(),
                stderr: error,
            });
        }

        Ok(SqlExecutionOutput {
            success: true,
            stdout: command_tag,
            stderr: String::new(),
        })
    }
}

impl Drop for PgWireSqlExecutor {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

fn sql_execution_error(context: String, output: &SqlExecutionOutput) -> io::Error {
    let mut details = output.stderr.trim().to_owned();
    if details.is_empty() {
        details = output.stdout.trim().to_owned();
    }
    if details.is_empty() {
        details = "sql execution failed without error output".to_owned();
    }
    io::Error::other(format!("{context}: {details}"))
}

fn read_frame(stream: &mut impl Read) -> io::Result<(u8, Vec<u8>)> {
    let mut message_type = [0_u8; 1];
    stream.read_exact(&mut message_type)?;
    let mut length_bytes = [0_u8; 4];
    stream.read_exact(&mut length_bytes)?;
    let length = i32::from_be_bytes(length_bytes);
    if length < 4 {
        return Err(io::Error::other(format!("invalid message length {length}")));
    }
    let mut payload = vec![0_u8; (length - 4) as usize];
    stream.read_exact(&mut payload)?;
    Ok((message_type[0], payload))
}

fn parse_error_message(payload: &[u8]) -> String {
    let mut message = None;
    let mut code = None;
    let mut index = 0;
    while index < payload.len() {
        let field_type = payload[index];
        index += 1;
        if field_type == 0 {
            break;
        }
        let start = index;
        while index < payload.len() && payload[index] != 0 {
            index += 1;
        }
        let value = String::from_utf8_lossy(&payload[start..index]).into_owned();
        index += usize::from(index < payload.len());
        match field_type {
            b'M' => message = Some(value),
            b'C' => code = Some(value),
            _ => {}
        }
    }

    match (code, message) {
        (Some(code), Some(message)) => format!("{code}: {message}"),
        (_, Some(message)) => message,
        _ => "server returned an error".to_owned(),
    }
}

fn parse_cstring(payload: &[u8]) -> String {
    let end = payload
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(payload.len());
    String::from_utf8_lossy(&payload[..end]).into_owned()
}

pub fn load_queries(config: &BenchmarkRunConfig) -> io::Result<Vec<QueryCase>> {
    let queries = if let Some(query_file) = &config.query_file {
        load_queries_from_file(query_file)?
    } else {
        let query_dir = config.queries_dir.clone().unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join(config.workload.name())
                .join("queries")
        });
        load_queries_from_dir(&query_dir)?
    };
    Ok(match config.workload {
        Workload::Tpch => render_tpch_query_parameters(queries),
        Workload::ClickBench => queries,
    })
}

pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut statement = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single_quote = false;

    while let Some(ch) = chars.next() {
        if ch == '-' && !in_single_quote && chars.peek() == Some(&'-') {
            chars.next();
            for comment_char in chars.by_ref() {
                if comment_char == '\n' {
                    statement.push('\n');
                    break;
                }
            }
            continue;
        }
        if ch == '\'' {
            statement.push(ch);
            if in_single_quote && chars.peek() == Some(&'\'') {
                statement.push(chars.next().expect("peeked quote"));
                continue;
            }
            in_single_quote = !in_single_quote;
            continue;
        }
        if ch == ';' && !in_single_quote {
            let trimmed = statement.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_owned());
            }
            statement.clear();
            continue;
        }
        statement.push(ch);
    }

    let trimmed = statement.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_owned());
    }
    statements
}

pub fn load_queries_from_file(query_file: &Path) -> io::Result<Vec<QueryCase>> {
    let sql = fs::read_to_string(query_file)?;
    let name = query_file
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    Ok(vec![QueryCase { name, sql }])
}

pub fn load_queries_from_dir(query_dir: &Path) -> io::Result<Vec<QueryCase>> {
    let mut paths = fs::read_dir(query_dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()?;
    paths.retain(|path| path.extension().is_some_and(|extension| extension == "sql"));
    paths.sort();

    paths
        .into_iter()
        .map(|path| {
            let sql = fs::read_to_string(&path)?;
            let name = path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            Ok(QueryCase { name, sql })
        })
        .collect()
}

fn render_tpch_query_parameters(queries: Vec<QueryCase>) -> Vec<QueryCase> {
    queries
        .into_iter()
        .map(|query| {
            let sql = tpch_query_parameters(&query.name)
                .map(|parameters| replace_template_parameters(&query.sql, parameters))
                .unwrap_or(query.sql);
            QueryCase { sql, ..query }
        })
        .collect()
}

fn replace_template_parameters(sql: &str, parameters: &[&str]) -> String {
    let mut rendered = sql.to_owned();
    for index in (1..=parameters.len()).rev() {
        rendered = rendered.replace(&format!(":{index}"), parameters[index - 1]);
    }
    rendered
}

fn tpch_query_parameters(query_name: &str) -> Option<&'static [&'static str]> {
    Some(match query_name {
        "q02" => &["15", "BRASS", "EUROPE"],
        "q03" => &["BUILDING", "1995-03-15"],
        "q04" => &["1993-07-01"],
        "q05" => &["ASIA", "1994-01-01"],
        "q06" => &["1994-01-01", "0.06", "24"],
        "q07" => &["FRANCE", "GERMANY"],
        "q08" => &["BRAZIL", "AMERICA", "ECONOMY ANODIZED STEEL"],
        "q09" => &["green"],
        "q10" => &["1993-10-01"],
        "q11" => &["GERMANY", "0.0001"],
        "q12" => &["MAIL", "SHIP", "1994-01-01"],
        "q13" => &["special", "requests"],
        "q14" => &["1995-09-01"],
        "q15" => &["1996-01-01"],
        "q16" => &[
            "Brand#45",
            "MEDIUM POLISHED",
            "49",
            "14",
            "23",
            "45",
            "19",
            "3",
            "36",
            "9",
        ],
        "q17" => &["Brand#23", "MED BOX"],
        "q18" => &["300"],
        "q19" => &["Brand#12", "Brand#23", "Brand#34", "1", "10", "20"],
        "q20" => &["forest", "1994-01-01", "CANADA"],
        "q21" => &["SAUDI ARABIA"],
        "q22" => &["13", "31", "23", "29", "30", "18", "17"],
        _ => return None,
    })
}

pub fn render_report(report: &BenchmarkReport) -> String {
    let mut output = String::new();
    output.push_str("workload,query,iteration,success,elapsed_ms,error\n");
    for result in &report.results {
        let error = if result.success {
            String::new()
        } else {
            result.stderr.clone()
        };
        output.push_str(&format!(
            "{},{},{},{},{},{}\n",
            report.workload.name(),
            result.query_name,
            result.iteration,
            result.success,
            result.elapsed.as_secs_f64() * 1000.0,
            csv_field(&error)
        ));
    }
    output
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '\n', '\r', '"']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}
