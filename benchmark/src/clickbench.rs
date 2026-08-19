use std::fs::{self, File};
use std::io::{self, BufWriter, Read};
use std::path::Path;

use flate2::read::GzDecoder;

const HITS_CSV_GZ_URL: &str = "https://datasets.clickhouse.com/hits_compatible/hits.csv.gz";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickBenchGenConfig {
    pub output_dir: std::path::PathBuf,
    pub overwrite: bool,
}

pub fn generate_clickbench_csv(config: &ClickBenchGenConfig) -> io::Result<()> {
    fs::create_dir_all(&config.output_dir)?;
    ensure_hits_csv_can_be_written(&config.output_dir, config.overwrite)?;

    let response = ureq::get(HITS_CSV_GZ_URL).call().map_err(|error| {
        io::Error::other(format!("failed to download ClickBench data: {error}"))
    })?;
    write_clickbench_csv_from_gzip(response.into_reader(), &config.output_dir, config.overwrite)
}

pub fn write_clickbench_csv_from_gzip<R: Read>(
    reader: R,
    output_dir: &Path,
    overwrite: bool,
) -> io::Result<()> {
    fs::create_dir_all(output_dir)?;
    ensure_hits_csv_can_be_written(output_dir, overwrite)?;

    let mut decoder = GzDecoder::new(reader);
    let hits_csv = output_dir.join("hits.csv");
    let mut writer = BufWriter::new(File::create(hits_csv)?);
    io::copy(&mut decoder, &mut writer)?;
    Ok(())
}

pub fn clickbench_load_sql(data_dir: &Path) -> String {
    let schema = include_str!("../clickbench/schema.sql");
    let load = include_str!("../clickbench/load.sql");
    format!(
        "{schema}\n{}",
        load.replace("${DATA_DIR}", &sql_path_literal(data_dir))
    )
}

fn sql_path_literal(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn ensure_hits_csv_can_be_written(output_dir: &Path, overwrite: bool) -> io::Result<()> {
    let hits_csv = output_dir.join("hits.csv");
    if hits_csv.exists() && !overwrite {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "{} already exists; pass --overwrite to replace it",
                hits_csv.display()
            ),
        ));
    }
    Ok(())
}
