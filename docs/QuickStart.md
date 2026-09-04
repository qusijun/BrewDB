# Quick Start

This guide starts a local BrewDB server and runs a simple SQL query.

## Build

Build requirements:

- Rust `1.88` or newer. The workspace uses Rust 2024 edition and records this
  requirement in `Cargo.toml`.
- Cargo from the matching Rust toolchain. With `rustup`, run
  `rustup update stable` if your local compiler is older.
- No system `protoc` installation is required for normal builds; the protobuf
  crate uses a vendored `protoc`.

Build the server and client binaries:

```bash
cargo build -p brewdb-bin --bin brewdbd --bin brewdb
```

Build the full workspace:

```bash
cargo build --workspace
```

## Start The Server

Run `brewdbd`:

```bash
cargo run -p brewdb-bin --bin brewdbd
```

By default, the server listens on `127.0.0.1:5432`.

To start with a config file, copy `configs/brewdbd.toml.template`, fill in the
values you need, then run:

```bash
cargo run -p brewdb-bin --bin brewdbd -- --config configs/brewdbd.toml.template
```

## Run SQL

From another terminal, execute one query:

```bash
cargo run -p brewdb-bin --bin brewdb -- -c "select 1"
```

Or start the interactive client:

```bash
cargo run -p brewdb-bin --bin brewdb
```

## Run Tests

Run core unit tests:

```bash
cargo test --workspace
```

Run SQL logic tests:

```bash
cargo test -p brewdb-sqllogictests --test integration_tests
```
