drop table if exists region;
create table if not exists region (
  r_regionkey bigint,
  r_name text,
  r_comment text
) with (file.format = parquet);

drop table if exists nation;
create table if not exists nation (
  n_nationkey bigint,
  n_name text,
  n_regionkey bigint,
  n_comment text
) with (file.format = parquet);

drop table if exists part;
create table if not exists part (
  p_partkey bigint,
  p_name text,
  p_mfgr text,
  p_brand text,
  p_type text,
  p_size int,
  p_container text,
  p_retailprice double,
  p_comment text
) with (file.format = parquet);

drop table if exists supplier;
create table if not exists supplier (
  s_suppkey bigint,
  s_name text,
  s_address text,
  s_nationkey bigint,
  s_phone text,
  s_acctbal double,
  s_comment text
) with (file.format = parquet);

drop table if exists customer;
create table if not exists customer (
  c_custkey bigint,
  c_name text,
  c_address text,
  c_nationkey bigint,
  c_phone text,
  c_acctbal double,
  c_mktsegment text,
  c_comment text
) with (file.format = parquet);

drop table if exists partsupp;
create table if not exists partsupp (
  ps_partkey bigint,
  ps_suppkey bigint,
  ps_availqty int,
  ps_supplycost double,
  ps_comment text
) with (file.format = parquet);

drop table if exists orders;
create table if not exists orders (
  o_orderkey bigint,
  o_custkey bigint,
  o_orderstatus text,
  o_totalprice double,
  o_orderdate date,
  o_orderpriority text,
  o_clerk text,
  o_shippriority int,
  o_comment text
) with (file.format = parquet);

drop table if exists lineitem;
create table if not exists lineitem (
  l_orderkey bigint,
  l_partkey bigint,
  l_suppkey bigint,
  l_linenumber int,
  l_quantity double,
  l_extendedprice double,
  l_discount double,
  l_tax double,
  l_returnflag text,
  l_linestatus text,
  l_shipdate date,
  l_commitdate date,
  l_receiptdate date,
  l_shipinstruct text,
  l_shipmode text,
  l_comment text
) with (file.format = parquet);
