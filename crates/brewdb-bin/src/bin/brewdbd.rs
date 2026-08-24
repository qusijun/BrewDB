use std::sync::Arc;
use tracing::{error, info};

fn main() {
    let server = match load_server() {
        Ok(server) => Arc::new(server),
        Err(error) => {
            eprintln!("brewdbd failed to start: {error}");
            std::process::exit(1);
        }
    };

    let listen_address = server.listen_address().to_owned();
    if let Err(error) = server.serve_tcp("pgwire", listen_address) {
        error!(
            target: "brewdbd",
            error = %error,
            pid = std::process::id(),
            "brewdbd failed to start"
        );
        eprintln!("brewdbd failed to start: {error}");
        std::process::exit(1);
    }
}

fn load_server() -> Result<brewdb_bin::server::BrewDbServer, brewdb_bin::server::BrewDbServerError>
{
    let mut args = std::env::args().skip(1);
    let mut config_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                config_path = args.next();
                break;
            }
            _ if arg.starts_with("--config=") => {
                config_path = Some(arg.trim_start_matches("--config=").to_owned());
                break;
            }
            _ => {}
        }
    }

    let config = match &config_path {
        Some(path) => brewdb_bin::server::load_system_config_file(path)?,
        None => brewdb_bin::server::bootstrap_system_config()?,
    };
    let bootstrap_warehouse = if config_path.is_none() {
        config
            .get_string(brewdb_catalog::CATALOG_PAIMON_WAREHOUSE_KEY)?
            .unwrap_or("")
            .to_owned()
    } else {
        String::new()
    };
    let _logging_config = brewdb_bin::server::init_logging_from_config(&config)?;
    let config_source = if config_path.is_some() {
        "file"
    } else {
        "bootstrap"
    };
    let config_path_for_log = config_path.as_deref().unwrap_or("");
    let server = brewdb_bin::server::BrewDbServer::from_system_config(config)?;
    if config_path.is_some() {
        info!(
            target: "brewdbd",
            pid = std::process::id(),
            config_source,
            config_path = config_path_for_log,
            listen_addr = server.listen_address(),
            "brewdbd initialized"
        );
    } else {
        info!(
            target: "brewdbd",
            pid = std::process::id(),
            config_source,
            bootstrap_warehouse = bootstrap_warehouse.as_str(),
            listen_addr = server.listen_address(),
            "brewdbd initialized"
        );
    }
    Ok(server)
}
