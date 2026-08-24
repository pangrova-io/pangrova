#!/usr/bin/env python3
"""Convert TPC-H/TPC-DS CSV data files to Parquet format.

Handles the pipe-delimited CSV format produced by dbgen/dsdgen and converts
to columnar Parquet with proper schema typing for efficient query execution.

Usage:
    python3 csv-to-parquet.py --benchmark tpch --input-dir /data/csv --output-dir /data/parquet
    python3 csv-to-parquet.py --benchmark tpcds --input-dir /data/csv --output-dir /data/parquet
"""

import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required. Install with: pip install pyarrow")
    sys.exit(2)

# ─── TPC-H Schema Definitions ────────────────────────────────────────────────

TPCH_SCHEMAS = {
    "lineitem": pa.schema([
        ("l_orderkey", pa.int64()),
        ("l_partkey", pa.int64()),
        ("l_suppkey", pa.int64()),
        ("l_linenumber", pa.int32()),
        ("l_quantity", pa.decimal128(15, 2)),
        ("l_extendedprice", pa.decimal128(15, 2)),
        ("l_discount", pa.decimal128(15, 2)),
        ("l_tax", pa.decimal128(15, 2)),
        ("l_returnflag", pa.string()),
        ("l_linestatus", pa.string()),
        ("l_shipdate", pa.date32()),
        ("l_commitdate", pa.date32()),
        ("l_receiptdate", pa.date32()),
        ("l_shipinstruct", pa.string()),
        ("l_shipmode", pa.string()),
        ("l_comment", pa.string()),
    ]),
    "orders": pa.schema([
        ("o_orderkey", pa.int64()),
        ("o_custkey", pa.int64()),
        ("o_orderstatus", pa.string()),
        ("o_totalprice", pa.decimal128(15, 2)),
        ("o_orderdate", pa.date32()),
        ("o_orderpriority", pa.string()),
        ("o_clerk", pa.string()),
        ("o_shippriority", pa.int32()),
        ("o_comment", pa.string()),
    ]),
    "customer": pa.schema([
        ("c_custkey", pa.int64()),
        ("c_name", pa.string()),
        ("c_address", pa.string()),
        ("c_nationkey", pa.int32()),
        ("c_phone", pa.string()),
        ("c_acctbal", pa.decimal128(15, 2)),
        ("c_mktsegment", pa.string()),
        ("c_comment", pa.string()),
    ]),
    "part": pa.schema([
        ("p_partkey", pa.int64()),
        ("p_name", pa.string()),
        ("p_mfgr", pa.string()),
        ("p_brand", pa.string()),
        ("p_type", pa.string()),
        ("p_size", pa.int32()),
        ("p_container", pa.string()),
        ("p_retailprice", pa.decimal128(15, 2)),
        ("p_comment", pa.string()),
    ]),
    "supplier": pa.schema([
        ("s_suppkey", pa.int64()),
        ("s_name", pa.string()),
        ("s_address", pa.string()),
        ("s_nationkey", pa.int32()),
        ("s_phone", pa.string()),
        ("s_acctbal", pa.decimal128(15, 2)),
        ("s_comment", pa.string()),
    ]),
    "partsupp": pa.schema([
        ("ps_partkey", pa.int64()),
        ("ps_suppkey", pa.int64()),
        ("ps_availqty", pa.int32()),
        ("ps_supplycost", pa.decimal128(15, 2)),
        ("ps_comment", pa.string()),
    ]),
    "nation": pa.schema([
        ("n_nationkey", pa.int32()),
        ("n_name", pa.string()),
        ("n_regionkey", pa.int32()),
        ("n_comment", pa.string()),
    ]),
    "region": pa.schema([
        ("r_regionkey", pa.int32()),
        ("r_name", pa.string()),
        ("r_comment", pa.string()),
    ]),
}

# TPC-H file name mapping (dbgen output names)
TPCH_FILE_MAP = {
    "lineitem.tbl": "lineitem",
    "orders.tbl": "orders",
    "customer.tbl": "customer",
    "part.tbl": "part",
    "supplier.tbl": "supplier",
    "partsupp.tbl": "partsupp",
    "nation.tbl": "nation",
    "region.tbl": "region",
}

# ─── TPC-DS Schema Definitions (key fact/dimension tables) ────────────────────

TPCDS_SCHEMAS = {
    "store_sales": pa.schema([
        ("ss_sold_date_sk", pa.int64()),
        ("ss_sold_time_sk", pa.int64()),
        ("ss_item_sk", pa.int64()),
        ("ss_customer_sk", pa.int64()),
        ("ss_cdemo_sk", pa.int64()),
        ("ss_hdemo_sk", pa.int64()),
        ("ss_addr_sk", pa.int64()),
        ("ss_store_sk", pa.int64()),
        ("ss_promo_sk", pa.int64()),
        ("ss_ticket_number", pa.int64()),
        ("ss_quantity", pa.int32()),
        ("ss_wholesale_cost", pa.decimal128(7, 2)),
        ("ss_list_price", pa.decimal128(7, 2)),
        ("ss_sales_price", pa.decimal128(7, 2)),
        ("ss_ext_discount_amt", pa.decimal128(7, 2)),
        ("ss_ext_sales_price", pa.decimal128(7, 2)),
        ("ss_ext_wholesale_cost", pa.decimal128(7, 2)),
        ("ss_ext_list_price", pa.decimal128(7, 2)),
        ("ss_ext_tax", pa.decimal128(7, 2)),
        ("ss_coupon_amt", pa.decimal128(7, 2)),
        ("ss_net_paid", pa.decimal128(7, 2)),
        ("ss_net_paid_inc_tax", pa.decimal128(7, 2)),
        ("ss_net_profit", pa.decimal128(7, 2)),
    ]),
    "date_dim": pa.schema([
        ("d_date_sk", pa.int64()),
        ("d_date_id", pa.string()),
        ("d_date", pa.date32()),
        ("d_month_seq", pa.int32()),
        ("d_week_seq", pa.int32()),
        ("d_quarter_seq", pa.int32()),
        ("d_year", pa.int32()),
        ("d_dow", pa.int32()),
        ("d_moy", pa.int32()),
        ("d_dom", pa.int32()),
        ("d_qoy", pa.int32()),
        ("d_fy_year", pa.int32()),
        ("d_fy_quarter_seq", pa.int32()),
        ("d_fy_week_seq", pa.int32()),
        ("d_day_name", pa.string()),
        ("d_quarter_name", pa.string()),
        ("d_holiday", pa.string()),
        ("d_weekend", pa.string()),
        ("d_following_holiday", pa.string()),
        ("d_first_dom", pa.int32()),
        ("d_last_dom", pa.int32()),
        ("d_same_day_ly", pa.int32()),
        ("d_same_day_lq", pa.int32()),
        ("d_current_day", pa.string()),
        ("d_current_week", pa.string()),
        ("d_current_month", pa.string()),
        ("d_current_quarter", pa.string()),
        ("d_current_year", pa.string()),
    ]),
    "item": pa.schema([
        ("i_item_sk", pa.int64()),
        ("i_item_id", pa.string()),
        ("i_rec_start_date", pa.date32()),
        ("i_rec_end_date", pa.date32()),
        ("i_item_desc", pa.string()),
        ("i_current_price", pa.decimal128(7, 2)),
        ("i_wholesale_cost", pa.decimal128(7, 2)),
        ("i_brand_id", pa.int32()),
        ("i_brand", pa.string()),
        ("i_class_id", pa.int32()),
        ("i_class", pa.string()),
        ("i_category_id", pa.int32()),
        ("i_category", pa.string()),
        ("i_manufact_id", pa.int32()),
        ("i_manufact", pa.string()),
        ("i_size", pa.string()),
        ("i_formulation", pa.string()),
        ("i_color", pa.string()),
        ("i_units", pa.string()),
        ("i_container", pa.string()),
        ("i_manager_id", pa.int32()),
        ("i_product_name", pa.string()),
    ]),
}


def convert_file(args):
    """Convert a single CSV file to Parquet."""
    input_path, output_path, schema, delimiter = args
    try:
        read_options = pcsv.ReadOptions(column_names=schema.names)
        parse_options = pcsv.ParseOptions(delimiter=delimiter)
        convert_options = pcsv.ConvertOptions(column_types=schema)

        table = pcsv.read_csv(
            input_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )
        pq.write_table(
            table,
            output_path,
            compression="snappy",
            row_group_size=1_000_000,
        )
        rows = table.num_rows
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"  ✓ {Path(input_path).name} → {Path(output_path).name} "
              f"({rows:,} rows, {size_mb:.1f} MB)")
        return True
    except Exception as e:
        print(f"  ✗ {Path(input_path).name}: {e}")
        return False


def convert_tpch(input_dir, output_dir, parallel):
    """Convert all TPC-H tables from CSV to Parquet."""
    print(f"\n>>> Converting TPC-H CSV → Parquet")
    print(f"    Input:  {input_dir}")
    print(f"    Output: {output_dir}")

    tasks = []
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for csv_name, table_name in TPCH_FILE_MAP.items():
        # Handle chunked files (lineitem.tbl.1, lineitem.tbl.2, etc.)
        chunk_files = sorted(input_path.glob(f"{csv_name}*"))
        if not chunk_files:
            print(f"  ⚠ {csv_name} not found, skipping")
            continue

        schema = TPCH_SCHEMAS[table_name]
        for chunk_file in chunk_files:
            suffix = chunk_file.suffix if chunk_file.suffix != ".tbl" else ""
            out_name = f"{table_name}{suffix}.parquet"
            tasks.append((
                str(chunk_file),
                str(output_path / out_name),
                schema,
                "|",
            ))

    with ProcessPoolExecutor(max_workers=parallel) as executor:
        results = list(executor.map(convert_file, tasks))

    success = sum(results)
    total = len(results)
    print(f"\n    Converted {success}/{total} files successfully.")
    return success == total


def convert_tpcds(input_dir, output_dir, parallel):
    """Convert all TPC-DS tables from CSV to Parquet."""
    print(f"\n>>> Converting TPC-DS CSV → Parquet")
    print(f"    Input:  {input_dir}")
    print(f"    Output: {output_dir}")

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    tasks = []
    # Find all .dat files produced by dsdgen
    dat_files = sorted(input_path.glob("*.dat"))
    if not dat_files:
        print("  ⚠ No .dat files found. Trying .csv extension...")
        dat_files = sorted(input_path.glob("*.csv"))

    for dat_file in dat_files:
        table_name = dat_file.stem.split("_1_")[0]  # Handle chunked names
        schema = TPCDS_SCHEMAS.get(table_name)
        if schema is None:
            # For tables without explicit schema, use auto-inference
            tasks.append((
                str(dat_file),
                str(output_path / f"{dat_file.stem}.parquet"),
                None,
                "|",
            ))
        else:
            tasks.append((
                str(dat_file),
                str(output_path / f"{dat_file.stem}.parquet"),
                schema,
                "|",
            ))

    # Filter out tasks with None schema (use simpler conversion)
    typed_tasks = [t for t in tasks if t[2] is not None]
    untyped_tasks = [t for t in tasks if t[2] is None]

    with ProcessPoolExecutor(max_workers=parallel) as executor:
        results = list(executor.map(convert_file, typed_tasks))

    # Handle untyped tables with auto-inference
    for input_file, output_file, _, delimiter in untyped_tasks:
        try:
            parse_options = pcsv.ParseOptions(delimiter=delimiter)
            table = pcsv.read_csv(input_file, parse_options=parse_options)
            pq.write_table(table, output_file, compression="snappy")
            results.append(True)
            print(f"  ✓ {Path(input_file).name} (auto-inferred schema)")
        except Exception as e:
            print(f"  ✗ {Path(input_file).name}: {e}")
            results.append(False)

    success = sum(results)
    total = len(results)
    print(f"\n    Converted {success}/{total} files successfully.")
    return success == total


def main():
    parser = argparse.ArgumentParser(description="Convert TPC CSV to Parquet")
    parser.add_argument("--benchmark", required=True, choices=["tpch", "tpcds"])
    parser.add_argument("--input-dir", required=True, help="Directory with CSV/TBL files")
    parser.add_argument("--output-dir", required=True, help="Output directory for Parquet")
    parser.add_argument("--parallel", type=int, default=4, help="Parallel workers")
    args = parser.parse_args()

    if args.benchmark == "tpch":
        success = convert_tpch(args.input_dir, args.output_dir, args.parallel)
    else:
        success = convert_tpcds(args.input_dir, args.output_dir, args.parallel)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
