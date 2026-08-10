# BrewDB

BrewDB is a lakehouse database engine built around a BrewDB-owned catalog,
DataFusion-based SQL planning/execution, and format-aware storage engines.

The current implementation focuses on the core query path and process boundary:

```text
brewdb client -> brewdbd -> frontend -> sql -> planner -> runtime -> execution
                                                     |
                                                     v
                                                  storage
```

## Positioning

BrewDB is intended to be a database engine for open lakehouse tables, not just a
thin SQL wrapper around a single file format.

Its core responsibilities are:

- own catalog metadata and table identity
- parse and bind SQL requests from client protocols
- build distributed query plans from DataFusion logical plans
- run fragment-local execution through DataFusion
- access lakehouse storage through BrewDB storage engines

Paimon is the first storage format being wired through this stack. The storage
adapter exposes Paimon tables as native DataFusion table providers inside
BrewDB, rather than depending on an external DataFusion fork.

## Core Architecture

The main crates are capability-oriented:

- `brewdb-common`: shared config, logging, diagnostics, and schema primitives
- `brewdb-catalog`: catalog model, catalog service, and table metadata
- `brewdb-frontend`: client sessions and protocol ingress such as pgwire
- `brewdb-sql`: SQL parsing and binding
- `brewdb-planner`: logical and distributed planning
- `brewdb-runtime`: scheduling, SQL driver, exchange, and execution orchestration
- `brewdb-execution`: execution-facing fragment contracts
- `brewdb-storage`: storage engine abstraction
- `brewdb-storage-paimon`: Paimon storage engine integration

Product entrypoints live under `bin/`:

- `brewdbd`: server process
- `brewdb`: SQL client

## Architecture Diagram

```text
                +-------------------+
                |       brewdb      |
                |   SQL client CLI   |
                +---------+---------+
                          |
                          | pgwire
                          v
                +-------------------+
                |      brewdbd      |
                |   server host     |
                +---------+---------+
                          |
                          v
                +-------------------+
                |   brewdb-frontend |
                | sessions/protocol |
                +---------+---------+
                          |
                          v
                +-------------------+
                |     brewdb-sql    |
                | parse + bind SQL  |
                +---------+---------+
                          |
                          v
                +-------------------+
                |   brewdb-planner  |
                | logical + distro  |
                +---------+---------+
                          |
                          v
              +----------------------+
              |    brewdb-runtime    |
              |  schedule + exchange |
              +----------+-----------+
                         / \
                        /   \
                       v     v
          +----------------+  +----------------+
          | worker / node 1 |  | worker / node N|
          | brewdb-execution|  | brewdb-execution|
          +--------+-------+  +--------+-------+
                   |                   |
                   v                   v
          +----------------+  +----------------+
          | brewdb-storage |  | brewdb-storage |
          | table engines   |  | table engines  |
          +--------+-------+  +--------+-------+
                   |                   |
                   v                   v
          +----------------+  +----------------+
          | Paimon / files |  | Paimon / files |
          +----------------+  +----------------+
```

## Build

Debug build:

```bash
cargo build -p brewdbd -p brewdb
```

Release build:

```bash
cargo build --release -p brewdbd -p brewdb
```

Run tests for the main binary boundary:

```bash
cargo test -p brewdb -p brewdbd
```

## Quick Start

Start the server:

```bash
./target/debug/brewdbd
```

In another terminal, run one query:

```bash
./target/debug/brewdb -c "select 1"
```

Or open the interactive client:

```bash
./target/debug/brewdb
```

Example interactive session:

```text
brewdb> select 1;
```

By default, `brewdbd` listens on `127.0.0.1:5432`, and `brewdb` connects to that
address. Override it with:

```bash
./target/debug/brewdb --host 127.0.0.1 --port 5432
```

## Status

BrewDB is still early-stage. The catalog, SQL path, runtime, pgwire shell,
client process, and Paimon read integration are being built up incrementally.
Expect some SQL shapes and storage operations to remain intentionally narrow
while the architecture settles.
