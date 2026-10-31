#!/usr/bin/env python3
"""Convert complete generator output into a validated, bounded-memory dataset."""

import argparse
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import re
import shutil
import sys
import tempfile

from tpc_common import ROOT, arrow_schema, digest, file_digest, read_json, table_definitions, write_json


def input_files(benchmark, directory, chunks):
    """Require exactly one complete layout; never combine full and chunked tables."""
    directory = Path(directory)
    tables = table_definitions(benchmark)
    found = {name: {} for name in tables}
    pattern = (r"(\w+)\.tbl(?:\.(\d+))?" if benchmark == "tpch"
               else r"(\w+?)(?:_(\d+)_(\d+))?\.dat")
    for path in sorted(directory.iterdir()):
        if path.suffix not in (".dat", ".tbl") and ".tbl." not in path.name:
            continue
        match = re.fullmatch(pattern, path.name)
        if match is None:
            raise ValueError(f"unrecognized generator file: {path.name}")
        name = match[1]
        if name == "dbgen_version" and benchmark == "tpcds":
            continue
        if name not in tables or not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"unknown, empty or invalid table file: {path.name}")
        child = int(match[2]) if match[2] else 0
        if benchmark == "tpcds" and child and int(match[3]) != chunks:
            raise ValueError(f"wrong total chunks: {path.name}")
        if child in found[name]:
            raise ValueError(f"duplicate chunk: {path.name}")
        found[name][child] = path
    for name, files in found.items():
        if not files:
            raise ValueError(f"missing table: {name}")
        expected = set(range(1, chunks + 1))
        if set(files) != {0} and set(files) != expected:
            raise ValueError(f"missing/mixed chunks for {name}: {sorted(files)}")
    return [(name, path) for name, files in found.items() for path in files.values()]


def file_record(path, rows):
    return {"path": path.name, "rows": rows, "bytes": path.stat().st_size, "sha256": file_digest(path)}


def convert_file(task):
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq
    name, source, output, columns, block_size, rows_per_file = task
    schema = arrow_schema(columns)
    # Every field, including the final one, is terminated with '|'.
    physical = schema.append(pa.field("__trailing", pa.string()))
    reader = pcsv.open_csv(
        source, read_options=pcsv.ReadOptions(column_names=physical.names, block_size=block_size),
        parse_options=pcsv.ParseOptions(delimiter="|", quote_char=False),
        convert_options=pcsv.ConvertOptions(column_types=physical, null_values=[""],
                                           strings_can_be_null=True))
    writer, count, part, files = None, 0, 0, []
    path = None
    try:
        for batch in reader:
            if batch.column(batch.num_columns - 1).null_count != batch.num_rows:
                raise ValueError(f"nonempty trailing field in {Path(source).name}")
            for index, field in enumerate(schema):
                if not field.nullable and batch.column(index).null_count:
                    raise ValueError(f"NULL in required column {name}.{field.name}")
            batch = pa.RecordBatch.from_arrays(list(batch.columns[:-1]), schema=schema)
            offset = 0
            while offset < batch.num_rows:
                if writer is None:
                    path = Path(output) / f"{Path(source).name}.part{part:05d}.parquet"
                    writer = pq.ParquetWriter(path, schema, compression="snappy")
                    count = 0
                length = min(rows_per_file - count, batch.num_rows - offset)
                writer.write_batch(batch.slice(offset, length), row_group_size=min(length, 131072))
                count += length
                offset += length
                if count == rows_per_file:
                    writer.close()
                    files.append(file_record(path, count))
                    writer = None
                    part += 1
        if writer is not None:
            writer.close()
            writer = None
            files.append(file_record(path, count))
    finally:
        reader.close()
        if writer is not None:
            writer.close()
    if not files:
        raise ValueError(f"no rows in {Path(source).name}")
    return name, {"input": Path(source).name, "inputSha256": file_digest(source), "files": files}


def convert(benchmark, input_dir, output_dir, chunks=1, parallel=1, scale_factor=1,
            block_size=8 * 1024 * 1024, rows_per_file=1000000, generator=None):
    for value in (chunks, parallel, scale_factor, block_size, rows_per_file):
        if type(value) is not int or value < 1:
            raise ValueError("chunks, parallel, scale, block size and rows per file must be positive integers")
    files = input_files(benchmark, input_dir, chunks)
    output = Path(output_dir)
    if output.exists():
        raise ValueError("output already exists; use a new destination, never reuse a partial dataset")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    tables = table_definitions(benchmark)
    try:
        tasks = [(name, str(path), str(staging), tables[name], block_size, rows_per_file)
                 for name, path in files]
        if parallel == 1:
            converted = list(map(convert_file, tasks))
        else:
            with ProcessPoolExecutor(max_workers=parallel) as executor:
                converted = list(executor.map(convert_file, tasks))
        records = {name: {"columns": columns, "inputs": [], "files": [], "rows": 0}
                   for name, columns in tables.items()}
        for name, record in converted:
            records[name]["inputs"].append({k: v for k, v in record.items() if k != "files"})
            records[name]["files"].extend(record["files"])
            records[name]["rows"] += sum(item["rows"] for item in record["files"])
        manifest = {"schemaVersion": 1, "kind": "tpc-parquet-dataset", "benchmark": benchmark,
                    "scaleFactor": scale_factor, "classification": "tpc-derived-engineering",
                    "format": "parquet", "compression": "snappy", "partitioning": "none",
                    "schemaSha256": file_digest(ROOT / "schemas" / f"{benchmark}.json"),
                    "generator": generator or {"provenance": "unverified-external-input"},
                    "conversion": {"blockSizeBytes": block_size, "rowsPerFile": rows_per_file},
                    "tables": records}
        manifest["datasetId"] = digest(manifest)
        write_json(staging / "dataset.json", manifest)
        (staging / "_SUCCESS").write_text(file_digest(staging / "dataset.json") + "\n")
        staging.rename(output)
        return manifest
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", required=True, choices=("tpch", "tpcds"))
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--parallel", type=int, default=1)
    parser.add_argument("--chunks", type=int, default=1)
    parser.add_argument("--scale-factor", type=int, default=1)
    parser.add_argument("--block-size", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--rows-per-file", type=int, default=1000000)
    parser.add_argument("--generator-manifest", type=Path)
    args = parser.parse_args()
    try:
        result = convert(args.benchmark, args.input_dir, args.output_dir, args.chunks,
                         args.parallel, args.scale_factor, args.block_size, args.rows_per_file,
                         read_json(args.generator_manifest) if args.generator_manifest else None)
        print(f"Validated {len(result['tables'])} tables; datasetId={result['datasetId']}")
        return 0
    except (OSError, ValueError, ImportError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
