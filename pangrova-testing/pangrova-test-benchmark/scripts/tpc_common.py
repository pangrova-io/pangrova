"""Strict, shared evidence I/O. Historical results are never implicitly upgraded."""

import hashlib
import json
import math
import os
from pathlib import Path
import tempfile


ROOT = Path(__file__).resolve().parent.parent


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path):
    with Path(path).open(encoding="utf-8") as source:
        return json.load(source, object_pairs_hook=unique_object,
                         parse_constant=lambda value: reject(f"non-finite JSON: {value}"))


def reject(message):
    raise ValueError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def file_digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", delete=False) as output:
            temporary = Path(output.name)
            json.dump(value, output, indent=2, allow_nan=False)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def positive(value, label, integer=False):
    if type(value) not in ((int,) if integer else (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{label} must be a positive {'integer' if integer else 'number'}")
    return value


def table_definitions(benchmark):
    if benchmark not in ("tpch", "tpcds"):
        raise ValueError("benchmark must be tpch or tpcds")
    return read_json(ROOT / "schemas" / f"{benchmark}.json")["tables"]


def arrow_schema(columns):
    import pyarrow as pa
    import re
    fields = []
    for column in columns:
        kind = column["type"]
        if kind.startswith(("varchar", "char")):
            datatype = pa.string()
        elif kind.startswith("decimal"):
            precision, scale = map(int, re.findall(r"\d+", kind))
            datatype = pa.decimal128(precision, scale)
        else:
            datatype = {"integer": pa.int32(), "bigint": pa.int64(), "date": pa.date32()}[kind]
        fields.append(pa.field(column["name"], datatype, nullable=column["nullable"]))
    return pa.schema(fields)
