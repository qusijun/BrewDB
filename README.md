# BrewDB

<p align="center">
  <img src="logo/brewdb.png" alt="BrewDB" width="520">
</p>

BrewDB is an experimental lakehouse database engine. It owns the SQL ingress,
catalog model, query planning boundary, runtime orchestration, and storage engine
integration while using DataFusion for logical and physical execution inside
fragments.

The first storage target is Apache Paimon. BrewDB exposes Paimon tables as native
DataFusion table providers and supports Parquet and Vortex data files through the
Paimon storage adapter.

## What Is Here

- SQL parser, binder, analyzer, and logical optimizer integration
- pgwire-compatible server and CLI client
- BrewDB-owned catalog and table metadata model
- fragment planning with standalone and distributed execution paths
- runtime scheduling, exchange, and fragment execution orchestration
- Paimon-backed table reads and append writes
- sqllogictest-style end-to-end tests
- benchmark tooling for TPC-H and ClickBench

## Architecture

```text
brewdb client
    |
    v
brewdbd
    |
    v
frontend/session -> SQL parser/binder -> logical optimizer
                         |                 |
                         v                 v
                   catalog service -> fragment planner
                         |                 |
                         v                 v
                catalog store backend  runtime coordinator
                   memory / fdb          /              \
                                        v                v
                                fragment executor   fragment executor
                                  worker/node 1       worker/node N
                                        |                |
                                        +---- exchange --+
                                        |                |
                                        v                v
                              storage table engine storage table engine
                                        |                |
                                        v                v
                                  Paimon / files    Paimon / files
```

The important boundary is that BrewDB owns planning, scheduling, catalog
identity, and storage-engine selection. DataFusion is used as the execution
engine for fragment-local plans. In distributed mode, exchange connects
fragment executors across workers instead of being a separate coordinator-side
plan branch.

## Repository Layout

- `crates/brewdb`: core library, including parser, catalog, planner, runtime,
  execution contracts, and storage engines
- `crates/brewdb-bin`: `brewdbd` server and `brewdb` SQL client binaries
- `crates/brewdb-sqllogictests`: end-to-end SQL logic test harness and test
  files
- `benchmark`: standalone benchmark crate for TPC-H and ClickBench
- `logo`: project assets

The benchmark crate is intentionally outside the root workspace so benchmark-only
dependencies do not affect normal builds.

## Build

Build the server and client:

```bash
cargo build -p brewdb-bin --bin brewdbd --bin brewdb
```

Build the core library:

```bash
cargo build -p brewdb
```

## Quick Start

Start the server:

```bash
cargo run -p brewdb-bin --bin brewdbd
```

Run a single query from another terminal:

```bash
cargo run -p brewdb-bin --bin brewdb -- -c "select 1"
```

Or start the interactive client:

```bash
cargo run -p brewdb-bin --bin brewdb
```

By default, `brewdbd` listens on `127.0.0.1:5432`.

## Tests

Run the core unit tests:

```bash
cargo test -p brewdb
```

Run the sqllogictest harness:

```bash
cargo test -p brewdb-sqllogictests --test integration_tests
```

The sqllogictest crate starts a shared `brewdbd` process and executes `.slt`
files from:

```text
crates/brewdb-sqllogictests/test_files
```

## Benchmarks

Generate small TPC-H data:

```bash
cargo run --manifest-path benchmark/Cargo.toml -- \
  gen tpch \
  --scale-factor 0.01 \
  --output /tmp/brewdb-bench/tpch \
  --overwrite
```

Run TPC-H against an already running `brewdbd`:

```bash
cargo run --manifest-path benchmark/Cargo.toml -- \
  run tpch \
  --data-dir /tmp/brewdb-bench/tpch \
  --paimon-file-format parquet \
  --iterations 1 \
  --host 127.0.0.1 \
  --port 5432
```

Use `--paimon-file-format vortex` to create benchmark tables backed by Vortex
data files. See [benchmark/README.md](benchmark/README.md) for the full TPC-H
and ClickBench workflow.

## Current Status

BrewDB is still early-stage. The core SQL path, catalog boundary, standalone
fast path, distributed fragment abstractions, Paimon integration, and benchmark
tooling are under active development. Expect unsupported SQL shapes and storage
operations while the architecture settles.
