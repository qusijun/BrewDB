use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkRunConfig {
    pub workload: Workload,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub data_dir: Option<PathBuf>,
    pub queries_dir: Option<PathBuf>,
    pub iterations: usize,
    pub setup: bool,
    pub brewdb_bin: PathBuf,
    pub brewdbd_bin: PathBuf,
    pub config_path: Option<PathBuf>,
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
    if config.setup {
        if let Some(data_dir) = &config.data_dir {
            let setup_sql = match config.workload {
                Workload::Tpch => tpch_load_sql(data_dir),
                Workload::ClickBench => clickbench_load_sql(data_dir),
            };
            execute_sql(config, &setup_sql)?;
        }
    }

    let queries = load_queries(config)?;
    let mut results = Vec::new();
    for iteration in 1..=config.iterations {
        for query in &queries {
            let started = Instant::now();
            let output = execute_sql(config, &query.sql)?;
            results.push(QueryRunResult {
                query_name: query.name.clone(),
                iteration,
                elapsed: started.elapsed(),
                success: output.status.success(),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }
    }

    Ok(BenchmarkReport {
        workload: config.workload.clone(),
        results,
    })
}

fn execute_sql(config: &BenchmarkRunConfig, sql: &str) -> io::Result<std::process::Output> {
    Command::new(&config.brewdb_bin)
        .arg("--host")
        .arg(&config.host)
        .arg("--port")
        .arg(config.port.to_string())
        .arg("--database")
        .arg(&config.database)
        .arg("--execute")
        .arg(sql)
        .output()
}

pub fn load_queries(config: &BenchmarkRunConfig) -> io::Result<Vec<QueryCase>> {
    let query_dir = config.queries_dir.clone().unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(config.workload.name())
            .join("queries")
    });
    load_queries_from_dir(&query_dir)
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

pub fn render_report(report: &BenchmarkReport) -> String {
    let mut output = String::new();
    output.push_str("workload,query,iteration,success,elapsed_ms\n");
    for result in &report.results {
        output.push_str(&format!(
            "{},{},{},{},{}\n",
            report.workload.name(),
            result.query_name,
            result.iteration,
            result.success,
            result.elapsed.as_secs_f64() * 1000.0
        ));
    }
    output
}
