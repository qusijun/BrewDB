use std::fs;
use std::path::{Path, PathBuf};

use brewdb_benchmark::benchmark::{BenchmarkRunConfig, Workload, load_queries_from_dir};
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
            iterations: 3,
            setup: true,
            brewdb_bin: PathBuf::from("../target/debug/brewdb"),
            brewdbd_bin: PathBuf::from("../target/debug/brewdbd"),
            config_path: None,
        })
    );
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
    let sql = tpch_load_sql(Path::new("/tmp/tpch"));

    assert!(sql.contains("create table if not exists lineitem"));
    assert!(sql.contains("copy from '/tmp/tpch/lineitem.csv' to lineitem"));
    assert!(sql.contains("with (format csv, header true)"));
    assert!(!sql.contains("${DATA_DIR}"));
}

#[test]
fn tpch_schema_and_load_sql_are_file_backed() {
    let benchmark_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    assert!(benchmark_dir.join("tpch/schema.sql").exists());
    assert!(benchmark_dir.join("tpch/load.sql").exists());
    assert!(benchmark_dir.join("tpch/queries/q01.sql").exists());
    assert!(benchmark_dir.join("clickbench/queries/q01.sql").exists());
}

#[test]
fn clickbench_schema_and_load_sql_are_file_backed() {
    let benchmark_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sql = clickbench_load_sql(Path::new("/tmp/clickbench"));

    assert!(benchmark_dir.join("clickbench/schema.sql").exists());
    assert!(benchmark_dir.join("clickbench/load.sql").exists());
    assert!(sql.contains("create table if not exists hits"));
    assert!(sql.contains("copy from '/tmp/clickbench/hits.csv' to hits"));
    assert!(sql.contains("with (format csv, header false)"));
    assert!(!sql.contains("${DATA_DIR}"));
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

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("brewdb-benchmark-{name}-{}", std::process::id()));
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();
    dir
}
