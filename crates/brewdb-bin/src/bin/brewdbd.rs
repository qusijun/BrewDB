use std::sync::Arc;

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
        eprintln!("brewdbd failed to start: {error}");
        std::process::exit(1);
    }
}

fn load_server() -> Result<brewdb_bin::server::BrewDbServer, brewdb_bin::server::BrewDbServerError>
{
    brewdb_bin::server::init_logging()?;
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

    match config_path {
        Some(path) => brewdb_bin::server::BrewDbServer::from_config_file(path),
        None => brewdb_bin::server::bootstrap(),
    }
}
