use std::process::ExitCode;

use brewdb_benchmark::benchmark::{render_report, run_benchmark};
use brewdb_benchmark::cli::{Command, parse_args, usage};
use brewdb_benchmark::clickbench::generate_clickbench_csv;
use brewdb_benchmark::tpch::generate_tpch_csv;

fn main() -> ExitCode {
    match parse_args(std::env::args_os()) {
        Ok(Command::GenerateTpch(config)) => match generate_tpch_csv(&config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("failed to generate TPC-H data: {error}");
                ExitCode::FAILURE
            }
        },
        Ok(Command::GenerateClickBench(config)) => match generate_clickbench_csv(&config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("failed to generate ClickBench data: {error}");
                ExitCode::FAILURE
            }
        },
        Ok(Command::Run(config)) => match run_benchmark(&config) {
            Ok(report) => {
                print!("{}", render_report(&report));
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("failed to run benchmark: {error}");
                ExitCode::FAILURE
            }
        },
        Ok(Command::Help) => {
            print!("{}", usage());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}\n\n{}", usage());
            ExitCode::FAILURE
        }
    }
}
