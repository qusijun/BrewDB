use std::ffi::OsString;
use std::path::PathBuf;

use crate::benchmark::{BenchmarkRunConfig, Workload};
use crate::clickbench::ClickBenchGenConfig;
use crate::tpch::TpchGenConfig;

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    GenerateTpch(TpchGenConfig),
    GenerateClickBench(ClickBenchGenConfig),
    Run(BenchmarkRunConfig),
    Help,
}

pub fn parse_args<I, S>(args: I) -> Result<Command, String>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut args = args
        .into_iter()
        .map(|arg| arg.into().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    if !args.is_empty() {
        args.remove(0);
    }
    parse_command(&args)
}

fn parse_command(args: &[String]) -> Result<Command, String> {
    match args {
        [] => Ok(Command::Help),
        [flag] if flag == "-h" || flag == "--help" => Ok(Command::Help),
        [cmd, workload, rest @ ..] if cmd == "gen" && workload == "tpch" => parse_gen_tpch(rest),
        [cmd, workload, rest @ ..] if cmd == "gen" && workload == "clickbench" => {
            parse_gen_clickbench(rest)
        }
        [cmd, workload, rest @ ..] if cmd == "run" => parse_run(workload, rest),
        _ => Err(format!("unsupported arguments: {}", args.join(" "))),
    }
}

fn parse_gen_clickbench(args: &[String]) -> Result<Command, String> {
    let mut output_dir = None;
    let mut overwrite = false;

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--output" | "-o" => output_dir = Some(PathBuf::from(take_value(arg, &mut iter)?)),
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown gen clickbench flag `{other}`")),
        }
    }

    Ok(Command::GenerateClickBench(ClickBenchGenConfig {
        output_dir: output_dir.ok_or_else(|| "gen clickbench requires --output".to_owned())?,
        overwrite,
    }))
}

fn parse_gen_tpch(args: &[String]) -> Result<Command, String> {
    let mut scale_factor = 1.0;
    let mut output_dir = None;
    let mut overwrite = false;

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--scale-factor" | "--sf" => {
                let value = take_value(arg, &mut iter)?;
                scale_factor = value
                    .parse()
                    .map_err(|_| format!("invalid scale factor `{value}`"))?;
            }
            "--output" | "-o" => output_dir = Some(PathBuf::from(take_value(arg, &mut iter)?)),
            "--overwrite" => overwrite = true,
            other => return Err(format!("unknown gen tpch flag `{other}`")),
        }
    }

    Ok(Command::GenerateTpch(TpchGenConfig {
        scale_factor,
        output_dir: output_dir.ok_or_else(|| "gen tpch requires --output".to_owned())?,
        overwrite,
    }))
}

fn parse_run(workload: &str, args: &[String]) -> Result<Command, String> {
    let workload =
        Workload::from_name(workload).ok_or_else(|| format!("unknown workload `{workload}`"))?;
    let mut config = BenchmarkRunConfig {
        workload,
        host: "127.0.0.1".to_owned(),
        port: 5432,
        database: "brewdb".to_owned(),
        data_dir: None,
        queries_dir: None,
        query_file: None,
        iterations: 1,
        setup: true,
        brewdb_bin: default_brewdb_bin(),
        config_path: None,
    };

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--host" => config.host = take_value(arg, &mut iter)?.to_owned(),
            "--port" => {
                let value = take_value(arg, &mut iter)?;
                config.port = value
                    .parse()
                    .map_err(|_| format!("invalid port `{value}`"))?;
            }
            "--database" | "-d" => config.database = take_value(arg, &mut iter)?.to_owned(),
            "--data-dir" => config.data_dir = Some(PathBuf::from(take_value(arg, &mut iter)?)),
            "--queries-dir" => {
                config.queries_dir = Some(PathBuf::from(take_value(arg, &mut iter)?))
            }
            "--query-file" => config.query_file = Some(PathBuf::from(take_value(arg, &mut iter)?)),
            "--iterations" | "-n" => {
                let value = take_value(arg, &mut iter)?;
                config.iterations = value
                    .parse()
                    .map_err(|_| format!("invalid iteration count `{value}`"))?;
            }
            "--no-setup" => config.setup = false,
            "--brewdb-bin" => config.brewdb_bin = PathBuf::from(take_value(arg, &mut iter)?),
            "--config" => config.config_path = Some(PathBuf::from(take_value(arg, &mut iter)?)),
            other => return Err(format!("unknown run flag `{other}`")),
        }
    }

    if config.iterations == 0 {
        return Err("--iterations must be greater than 0".to_owned());
    }
    if config.query_file.is_some() && config.queries_dir.is_some() {
        return Err("cannot specify both --query-file and --queries-dir".to_owned());
    }

    Ok(Command::Run(config))
}

fn take_value<'a>(
    flag: &str,
    iter: &mut impl Iterator<Item = &'a String>,
) -> Result<&'a str, String> {
    iter.next()
        .map(String::as_str)
        .ok_or_else(|| format!("{flag} requires a value"))
}

pub fn usage() -> &'static str {
    "Usage:\n  benchmark gen tpch --scale-factor <sf> --output <dir> [--overwrite]\n  benchmark gen clickbench --output <dir> [--overwrite]\n  benchmark run <tpch|clickbench> [--data-dir <dir>] [--queries-dir <dir>] [--query-file <file>] [--iterations <n>] [--host <host>] [--port <port>] [--database <db>] [--no-setup]\n"
}

fn default_brewdb_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("target")
        .join("debug")
        .join("brewdb")
}
