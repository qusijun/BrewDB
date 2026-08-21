use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use brewdb_benchmark::benchmark::{BenchmarkReport, QueryRunResult, render_report};
use brewdb_benchmark::benchmark::{
    BenchmarkRunConfig, PaimonFileFormat, Workload, load_queries, load_queries_from_dir,
    split_sql_statements,
};
use brewdb_benchmark::cli::{Command, parse_args};
use brewdb_benchmark::clickbench::{
    ClickBenchGenConfig, clickbench_load_sql, write_clickbench_csv_from_gzip,
};
use brewdb_benchmark::tpch::{TpchGenConfig, generate_tpch_csv, tpch_load_sql};

#[test]
fn parse_gen_tpch_command() {
    let command = parse_args([
        "benchmark",
        "gen",
        "tpch",
        "--scale-factor",
        "0.01",
        "--output",
        "/tmp/tpch",
    ])
    .unwrap();

    assert_eq!(
        command,
        Command::GenerateTpch(TpchGenConfig {
            scale_factor: 0.01,
            output_dir: PathBuf::from("/tmp/tpch"),
            overwrite: false,
        })
    );
}

#[test]
fn parse_gen_clickbench_command() {
    let command = parse_args([
        "benchmark",
        "gen",
        "clickbench",
        "--output",
        "/tmp/clickbench",
        "--overwrite",
    ])
    .unwrap();

    assert_eq!(
        command,
        Command::GenerateClickBench(ClickBenchGenConfig {
            output_dir: PathBuf::from("/tmp/clickbench"),
            overwrite: true,
        })
    );
}

#[test]
fn parse_run_tpch_command() {
    let command = parse_args([
        "benchmark",
        "run",
        "tpch",
        "--data-dir",
        "/tmp/tpch",
        "--iterations",
        "3",
    ])
    .unwrap();

    assert_eq!(
        command,
        Command::Run(BenchmarkRunConfig {
            workload: Workload::Tpch,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            database: "brewdb".to_owned(),
            data_dir: Some(PathBuf::from("/tmp/tpch")),
            queries_dir: None,
            query_file: None,
            iterations: 3,
            setup: true,
            brewdb_bin: default_brewdb_bin(),
            config_path: None,
            paimon_file_format: PaimonFileFormat::Parquet,
        })
    );
}

#[test]
fn parse_run_accepts_paimon_file_format() {
    let command = parse_args([
        "benchmark",
        "run",
        "tpch",
        "--data-dir",
        "/tmp/tpch",
        "--paimon-file-format",
        "vortex",
    ])
    .unwrap();

    assert_eq!(
        command,
        Command::Run(BenchmarkRunConfig {
            workload: Workload::Tpch,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            database: "brewdb".to_owned(),
            data_dir: Some(PathBuf::from("/tmp/tpch")),
            queries_dir: None,
            query_file: None,
            iterations: 1,
            setup: true,
            brewdb_bin: default_brewdb_bin(),
            config_path: None,
            paimon_file_format: PaimonFileFormat::Vortex,
        })
    );
}

#[test]
fn parse_run_rejects_unknown_paimon_file_format() {
    let error =
        parse_args(["benchmark", "run", "tpch", "--paimon-file-format", "orc"]).unwrap_err();

    assert!(error.contains("unsupported file format `orc`"));
}

#[test]
fn parse_run_tpch_query_file_command() {
    let command = parse_args([
        "benchmark",
        "run",
        "tpch",
        "--query-file",
        "/tmp/q01.sql",
        "--no-setup",
    ])
    .unwrap();

    assert_eq!(
        command,
        Command::Run(BenchmarkRunConfig {
            workload: Workload::Tpch,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            database: "brewdb".to_owned(),
            data_dir: None,
            queries_dir: None,
            query_file: Some(PathBuf::from("/tmp/q01.sql")),
            iterations: 1,
            setup: false,
            brewdb_bin: default_brewdb_bin(),
            config_path: None,
            paimon_file_format: PaimonFileFormat::Parquet,
        })
    );
}

#[test]
fn parse_run_rejects_brewdbd_bin_command() {
    let error =
        parse_args(["benchmark", "run", "tpch", "--brewdbd-bin", "/tmp/brewdbd"]).unwrap_err();

    assert!(error.contains("unknown run flag `--brewdbd-bin`"));
}

#[test]
fn generate_tpch_csv_writes_all_tables_with_headers() {
    let output_dir = fresh_dir("tpch_csv");

    generate_tpch_csv(&TpchGenConfig {
        scale_factor: 0.001,
        output_dir: output_dir.clone(),
        overwrite: false,
    })
    .unwrap();

    for table in [
        "region", "nation", "part", "supplier", "customer", "partsupp", "orders", "lineitem",
    ] {
        let path = output_dir.join(format!("{table}.csv"));
        assert!(path.exists(), "{} should exist", path.display());
        let contents = fs::read_to_string(&path).unwrap();
        assert!(
            contents
                .lines()
                .next()
                .is_some_and(|line| line.contains('_')),
            "{} should start with a header",
            path.display()
        );
    }
}

#[test]
fn tpch_load_sql_points_copy_from_to_generated_csvs() {
    let sql = tpch_load_sql(Path::new("/tmp/tpch"), PaimonFileFormat::Parquet);

    assert!(sql.contains("create table if not exists lineitem"));
    assert!(sql.contains(") with (file.format = parquet);"));
    assert!(sql.contains("copy from '/tmp/tpch/lineitem.csv' to lineitem"));
    assert!(sql.contains("with (format csv, header true)"));
    assert!(!sql.contains("${DATA_DIR}"));
}

#[test]
fn tpch_load_sql_can_select_vortex_paimon_file_format() {
    let sql = tpch_load_sql(Path::new("/tmp/tpch"), PaimonFileFormat::Vortex);

    assert!(sql.contains(") with (file.format = vortex);"));
    assert!(!sql.contains("file.format = parquet"));
}

#[test]
fn tpch_schema_and_load_sql_are_file_backed() {
    let benchmark_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    assert!(benchmark_dir.join("tpch/parquet/schema.sql").exists());
    assert!(benchmark_dir.join("tpch/vortex/schema.sql").exists());
    assert!(benchmark_dir.join("tpch/load.sql").exists());
    for query in 1..=22 {
        assert!(
            benchmark_dir
                .join(format!("tpch/queries/q{query:02}.sql"))
                .exists()
        );
    }
    for query in 1..=43 {
        assert!(
            benchmark_dir
                .join(format!("clickbench/queries/q{query:02}.sql"))
                .exists()
        );
    }
}

#[test]
fn tpch_queries_are_loaded_with_deterministic_parameters() {
    let config = BenchmarkRunConfig {
        workload: Workload::Tpch,
        host: "127.0.0.1".to_owned(),
        port: 5432,
        database: "brewdb".to_owned(),
        data_dir: None,
        queries_dir: None,
        query_file: None,
        iterations: 1,
        setup: false,
        brewdb_bin: default_brewdb_bin(),
        config_path: None,
        paimon_file_format: PaimonFileFormat::Parquet,
    };

    let queries = load_queries(&config).unwrap();

    assert_eq!(queries.len(), 22);
    for query in queries {
        assert!(
            !query.sql.contains(":1"),
            "{} should not contain template parameters: {}",
            query.name,
            query.sql
        );
    }
}

#[test]
fn clickbench_schema_and_load_sql_are_file_backed() {
    let benchmark_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sql = clickbench_load_sql(Path::new("/tmp/clickbench"), PaimonFileFormat::Parquet);

    assert!(benchmark_dir.join("clickbench/parquet/schema.sql").exists());
    assert!(benchmark_dir.join("clickbench/vortex/schema.sql").exists());
    assert!(benchmark_dir.join("clickbench/load.sql").exists());
    assert!(sql.contains("create table if not exists hits"));
    assert!(sql.contains("IsRefresh smallint"));
    assert!(sql.contains(") with (file.format = parquet);"));
    assert!(sql.contains("copy from '/tmp/clickbench/hits.csv' to hits"));
    assert!(sql.contains("with (format csv, header false)"));
    assert!(!sql.contains("${DATA_DIR}"));
}

#[test]
fn clickbench_queries_are_loaded_from_builtin_directory() {
    let config = BenchmarkRunConfig {
        workload: Workload::ClickBench,
        host: "127.0.0.1".to_owned(),
        port: 5432,
        database: "brewdb".to_owned(),
        data_dir: None,
        queries_dir: None,
        query_file: None,
        iterations: 1,
        setup: false,
        brewdb_bin: default_brewdb_bin(),
        config_path: None,
        paimon_file_format: PaimonFileFormat::Parquet,
    };

    let queries = load_queries(&config).unwrap();

    assert_eq!(queries.len(), 43);
    assert_eq!(queries[0].name, "q01");
    assert!(queries[0].sql.contains("COUNT(*) FROM hits"));
    assert_eq!(queries[42].name, "q43");
    assert!(queries[42].sql.contains("DATE_TRUNC('minute', EventTime)"));
}

#[test]
fn clickbench_load_sql_can_select_vortex_paimon_file_format() {
    let sql = clickbench_load_sql(Path::new("/tmp/clickbench"), PaimonFileFormat::Vortex);

    assert!(sql.contains(") with (file.format = vortex);"));
    assert!(!sql.contains("file.format = parquet"));
}

#[test]
fn write_clickbench_csv_from_gzip_extracts_hits_csv() {
    let output_dir = fresh_dir("clickbench_csv");
    let mut gzipped = Vec::new();
    {
        let mut encoder =
            flate2::write::GzEncoder::new(&mut gzipped, flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, b"1,2,3\n").unwrap();
        encoder.finish().unwrap();
    }

    write_clickbench_csv_from_gzip(&gzipped[..], &output_dir, false).unwrap();

    assert_eq!(
        fs::read_to_string(output_dir.join("hits.csv")).unwrap(),
        "1,2,3\n"
    );
}

#[test]
fn write_clickbench_csv_from_gzip_rejects_existing_file_without_overwrite() {
    let output_dir = fresh_dir("clickbench_existing_csv");
    fs::write(output_dir.join("hits.csv"), "old").unwrap();

    let error = write_clickbench_csv_from_gzip(&[][..], &output_dir, false).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        fs::read_to_string(output_dir.join("hits.csv")).unwrap(),
        "old"
    );
}

#[test]
fn render_report_includes_failure_error_message() {
    let report = BenchmarkReport {
        workload: Workload::Tpch,
        results: vec![QueryRunResult {
            query_name: "q01".to_owned(),
            iteration: 1,
            elapsed: std::time::Duration::from_millis(12),
            success: false,
            stdout: String::new(),
            stderr: "boom, broken \"plan\"".to_owned(),
        }],
    };

    let output = render_report(&report);

    assert!(output.contains("workload,query,iteration,success,elapsed_ms,error"));
    assert!(output.contains("\"boom, broken \"\"plan\"\"\""));
}

#[test]
fn split_sql_statements_splits_file_backed_setup_sql() {
    let statements = split_sql_statements(
        "create table t(a text);\ncopy from '/tmp/a;b.csv' to t;\n-- trailing\n",
    );

    assert_eq!(statements.len(), 2);
    assert_eq!(statements[0], "create table t(a text)");
    assert_eq!(statements[1], "copy from '/tmp/a;b.csv' to t");
}

#[test]
fn run_benchmark_fails_when_setup_statement_fails() {
    let bin_dir = fresh_dir("failing_brewdb_bin");
    let brewdb_bin = bin_dir.join("brewdb");
    write_fake_brewdb_script(&brewdb_bin, 1, "setup failed");

    let error = brewdb_benchmark::benchmark::run_benchmark(&BenchmarkRunConfig {
        workload: Workload::Tpch,
        host: "127.0.0.1".to_owned(),
        port: 5432,
        database: "brewdb".to_owned(),
        data_dir: Some(PathBuf::from("/tmp/tpch")),
        queries_dir: None,
        query_file: Some(PathBuf::from("benchmark/tpch/queries/q01.sql")),
        iterations: 1,
        setup: true,
        brewdb_bin,
        config_path: None,
        paimon_file_format: PaimonFileFormat::Parquet,
    })
    .unwrap_err();

    assert!(error.to_string().contains("setup statement 1 failed"));
    assert!(error.to_string().contains("setup failed"));
}

#[test]
fn load_queries_from_dir_reads_sql_files_in_name_order() {
    let query_dir = fresh_dir("queries");
    fs::write(query_dir.join("q02.sql"), "select 2;").unwrap();
    fs::write(query_dir.join("q01.sql"), "select 1;").unwrap();
    fs::write(query_dir.join("notes.txt"), "ignored").unwrap();

    let queries = load_queries_from_dir(&query_dir).unwrap();

    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0].name, "q01");
    assert_eq!(queries[0].sql, "select 1;");
    assert_eq!(queries[1].name, "q02");
    assert_eq!(queries[1].sql, "select 2;");
}

#[test]
fn load_queries_from_file_reads_single_sql_file() {
    let query_file = fresh_dir("single_query").join("q01.sql");
    fs::write(&query_file, "select 1;").unwrap();

    let queries = brewdb_benchmark::benchmark::load_queries_from_file(&query_file).unwrap();

    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].name, "q01");
    assert_eq!(queries[0].sql, "select 1;");
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("brewdb-benchmark-{name}-{}", std::process::id()));
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn default_brewdb_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("target")
        .join("debug")
        .join("brewdb")
}

fn write_fake_brewdb_script(path: &Path, exit_code: i32, stderr: &str) {
    fs::write(
        path,
        format!("#!/bin/sh\necho {stderr:?} >&2\nexit {exit_code}\n"),
    )
    .unwrap();
    ProcessCommand::new("chmod")
        .arg("+x")
        .arg(path)
        .status()
        .unwrap();
}
