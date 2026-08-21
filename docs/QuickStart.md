# Quick Start

This guide starts a local BrewDB server and runs a simple SQL query.

## Build

Build the server and client binaries:

```bash
cargo build -p brewdb-bin --bin brewdbd --bin brewdb
```

## Start The Server

Run `brewdbd`:

```bash
cargo run -p brewdb-bin --bin brewdbd
```

By default, the server listens on `127.0.0.1:5432`.

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
cargo test -p brewdb
```

Run SQL logic tests:

```bash
cargo test -p brewdb-sqllogictests --test integration_tests
```
