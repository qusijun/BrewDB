use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use brewdb_sqllogictests::ServerHarness;

const FIXTURE_DIR: &str = "test_files";

fn main() -> ExitCode {
    let filters = std::env::args().skip(1).collect::<Vec<_>>();
    let fixtures = slt_fixtures()
        .into_iter()
        .filter(|fixture| {
            let fixture = fixture.to_string_lossy();
            filters.is_empty() || filters.iter().any(|filter| fixture.contains(filter))
        })
        .collect::<Vec<_>>();

    if fixtures.is_empty() {
        eprintln!("no sqllogictest fixtures matched filters: {filters:?}");
        return ExitCode::FAILURE;
    }

    let harness = ServerHarness::start();
    for fixture in fixtures {
        if let Err(error) = harness.run_fixture(&fixture) {
            eprintln!("fixture {} failed:\n{error}", fixture.display());
            return ExitCode::FAILURE;
        }
    }

    ExitCode::SUCCESS
}

fn slt_fixtures() -> Vec<PathBuf> {
    let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_DIR);
    let mut fixtures = fs::read_dir(&fixture_dir)
        .unwrap_or_else(|error| {
            panic!(
                "failed to read sqllogictest fixture directory {}: {error}",
                fixture_dir.display()
            )
        })
        .map(|entry| {
            entry
                .expect("failed to read sqllogictest fixture entry")
                .path()
        })
        .filter(|path| path.extension().is_some_and(|extension| extension == "slt"))
        .collect::<Vec<_>>();
    fixtures.sort();
    fixtures
}
