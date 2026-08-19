use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use tpchgen::csv::{
    CustomerCsv, LineItemCsv, NationCsv, OrderCsv, PartCsv, PartSuppCsv, RegionCsv, SupplierCsv,
};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

#[derive(Debug, Clone, PartialEq)]
pub struct TpchGenConfig {
    pub scale_factor: f64,
    pub output_dir: PathBuf,
    pub overwrite: bool,
}

pub fn generate_tpch_csv(config: &TpchGenConfig) -> io::Result<()> {
    fs::create_dir_all(&config.output_dir)?;

    write_table(
        &config.output_dir.join("region.csv"),
        RegionCsv::header(),
        config.overwrite,
        || RegionGenerator::default().iter().map(RegionCsv::new),
    )?;
    write_table(
        &config.output_dir.join("nation.csv"),
        NationCsv::header(),
        config.overwrite,
        || NationGenerator::default().iter().map(NationCsv::new),
    )?;
    write_table(
        &config.output_dir.join("part.csv"),
        PartCsv::header(),
        config.overwrite,
        || {
            PartGenerator::new(config.scale_factor, 1, 1)
                .iter()
                .map(PartCsv::new)
        },
    )?;
    write_table(
        &config.output_dir.join("supplier.csv"),
        SupplierCsv::header(),
        config.overwrite,
        || {
            SupplierGenerator::new(config.scale_factor, 1, 1)
                .iter()
                .map(SupplierCsv::new)
        },
    )?;
    write_table(
        &config.output_dir.join("customer.csv"),
        CustomerCsv::header(),
        config.overwrite,
        || {
            CustomerGenerator::new(config.scale_factor, 1, 1)
                .iter()
                .map(CustomerCsv::new)
        },
    )?;
    write_table(
        &config.output_dir.join("partsupp.csv"),
        PartSuppCsv::header(),
        config.overwrite,
        || {
            PartSuppGenerator::new(config.scale_factor, 1, 1)
                .iter()
                .map(PartSuppCsv::new)
        },
    )?;
    write_table(
        &config.output_dir.join("orders.csv"),
        OrderCsv::header(),
        config.overwrite,
        || {
            OrderGenerator::new(config.scale_factor, 1, 1)
                .iter()
                .map(OrderCsv::new)
        },
    )?;
    write_table(
        &config.output_dir.join("lineitem.csv"),
        LineItemCsv::header(),
        config.overwrite,
        || {
            LineItemGenerator::new(config.scale_factor, 1, 1)
                .iter()
                .map(LineItemCsv::new)
        },
    )?;

    Ok(())
}

fn write_table<I, R, F>(path: &Path, header: &str, overwrite: bool, rows: F) -> io::Result<()>
where
    I: IntoIterator<Item = R>,
    R: std::fmt::Display,
    F: FnOnce() -> I,
{
    if path.exists() && !overwrite {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "{} already exists; pass --overwrite to replace it",
                path.display()
            ),
        ));
    }

    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "{header}")?;
    for row in rows() {
        writeln!(writer, "{row}")?;
    }
    writer.flush()
}

pub fn tpch_load_sql(data_dir: &Path) -> String {
    let schema = include_str!("../tpch/schema.sql");
    let load = include_str!("../tpch/load.sql");
    format!(
        "{schema}\n{}",
        load.replace("${DATA_DIR}", &sql_path_literal(data_dir))
    )
}

fn sql_path_literal(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}
