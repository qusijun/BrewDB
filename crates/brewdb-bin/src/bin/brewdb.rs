fn main() {
    if let Err(error) = brewdb_bin::client::run() {
        eprintln!("brewdb failed: {error}");
        std::process::exit(1);
    }
}
