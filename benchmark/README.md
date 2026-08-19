# BrewDB Benchmark

This directory contains BrewDB benchmark tooling as an independent Cargo crate.
It is intentionally not a member of the root workspace so benchmark-only
dependencies do not affect normal BrewDB builds.

## Generate TPC-H Data

TPC-H data generation is backed by the Rust `tpchgen` crate, a Rust
implementation of the TPC-H dbgen logic.

```bash
cargo run --manifest-path benchmark/Cargo.toml -- \
  gen tpch \
  --scale-factor 0.01 \
  --output /tmp/brewdb-bench/tpch \
  --overwrite
```

The command writes CSV files with headers:

- `region.csv`
- `nation.csv`
- `part.csv`
- `supplier.csv`
- `customer.csv`
- `partsupp.csv`
- `orders.csv`
- `lineitem.csv`

## Download ClickBench Data

ClickBench data generation downloads the official `hits.csv.gz` dataset and
decompresses it into `hits.csv`:

```bash
cargo run --manifest-path benchmark/Cargo.toml -- \
  gen clickbench \
  --output /tmp/brewdb-bench/clickbench \
  --overwrite
```

The command writes:

- `hits.csv`

## Run A Benchmark

Start `brewdbd` separately, then run a workload through the BrewDB CLI:

```bash
cargo run --manifest-path benchmark/Cargo.toml -- \
  run tpch \
  --data-dir /tmp/brewdb-bench/tpch \
  --iterations 3 \
  --host 127.0.0.1 \
  --port 5432
```

For TPC-H, setup is enabled by default. It runs generated DDL and `COPY FROM`
statements before executing query files. The setup SQL is file-backed:

- `benchmark/tpch/schema.sql`
- `benchmark/tpch/load.sql`

For ClickBench, setup expects a header-less CSV file at:

- `<data-dir>/hits.csv`

The setup SQL is also file-backed:

- `benchmark/clickbench/schema.sql`
- `benchmark/clickbench/load.sql`

To run existing tables without loading data:

```bash
cargo run --manifest-path benchmark/Cargo.toml -- \
  run tpch \
  --no-setup \
  --queries-dir benchmark/tpch/queries
```

Results are emitted as CSV:

```text
workload,query,iteration,success,elapsed_ms
tpch,q01,1,true,12.34
```

## Workloads

Built-in workloads are query-directory based:

- `benchmark/tpch/schema.sql`
- `benchmark/tpch/load.sql`
- `benchmark/tpch/queries/*.sql`
- `benchmark/clickbench/schema.sql`
- `benchmark/clickbench/load.sql`
- `benchmark/clickbench/queries/*.sql`

You can point either workload at another directory with `--queries-dir`.
Files are sorted by name and every `.sql` file is executed once per iteration.
