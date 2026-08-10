fn main() {
    if let Err(error) = brewdb::run() {
        eprintln!("brewdb failed: {error}");
        std::process::exit(1);
    }
}
