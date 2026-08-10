use std::sync::Arc;

fn main() {
    let server = match brewdbd::bootstrap() {
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
