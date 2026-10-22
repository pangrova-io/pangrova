---
title: "Paimon"
description: "Apache Paimon connector configuration"
---

# Paimon

Configure the Paimon connector for streaming lakehouse tables.


## Paimon Connector

Apache Paimon provides streaming lakehouse capabilities with real-time data ingestion.

### Catalog Configuration

The YAML is a reference format; use SQL registration for the runtime catalog.
Automatic startup registration remains planned.

```yaml
# config/catalog/my-paimon.yml
connector: paimon

properties:
  warehouse: "s3://my-bucket/paimon-warehouse"
  # metastore: filesystem  # filesystem (default), hive
```

### SQL Registration

```sql
CREATE CATALOG my_paimon
USING paimon
WITH (
  'warehouse' = 's3://my-bucket/paimon-warehouse'
);
```

### Physical file formats

Native scan splits retain the Paimon SDK's exact snapshot ID, physical file
location, format, byte size and file schema ID. Row counts are not byte ranges;
unknown formats and missing physical metadata fail explicitly.

Paimon 1.4.1 includes a `paimon-lance` plugin for single physical Lance files,
using Lance SDK 0.39.0. Paimon continues to own the table namespace, manifests
and snapshot commit. This is distinct from a Lance Dataset directory and its
version manifests. See the [released plugin dependency contract](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/pom.xml)
and [physical file writer](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/src/main/java/org/apache/paimon/format/lance/jni/LanceWriter.java).

The SDK plugin is a test-only dependency, not a second production execution
runtime. The current source connects the generic `physical_file_scan` read unit
to the repository-pinned Native reader ABI and public Rust SDK decoder. It retains the original
footer (SDK 0.39 defaults to the SDK-defined 0.3 spelling of the 2.0 encoding),
checks snapshot byte/row bounds, and preserves physical row positions. Worker
storage settings are resolved per authorized catalog and tenant; no global
object-store credentials or path rewrites are used.

Per-file deletion metadata contains the shared object's exact offset, length,
cardinality and physical-row domain. Native validates the SDK bitmap container,
CRC and row bounds. Deduplicate merge reads persisted sequence/kind fields before
projecting user columns, so a delete winner cannot resurrect an older row.
Historical metadata resolves the schema ID from the pinned Paimon snapshot.

Each physical file carries its own immutable field-ID mapping and source types.
Renamed fields remain bound to their original IDs; a new field reusing an old
name reads NULL from older files rather than borrowing that old field's values.
Numeric widening and exact widening of decimal precision/scale are applied
before primary-key merge. A query projecting no primary-key columns still reads
the keys internally from the canonical logical scan schema.

The current basic merge policy includes deduplicate/first-row `ignore-delete`,
partial-update delete reset, and aggregation delete reset and retractions. The
aggregation default is the SDK's `last_non_null_value`; `collect` operates on
arrays, `merge_map` overrides/retracts keys, and default `nested_update` operates
on arrays of rows. Numeric sum/min/max/product and decimal sum/min/max are
bounded and checked, not silently coerced. A key group or complex aggregate
exceeding 65,536 versions/elements fails explicitly rather than growing memory
without bound.

Unrepresented SDK policies are rejected while planning splits, before dispatch:
partial-update sequence groups or field aggregators, collect `distinct`, nested
keys/count limits, aggregate `ignore-retract`, custom list delimiters, decimal
product, non-numeric min/max, and defaults for fields absent from an old file.
Those options are not completed support; rejecting them prevents incorrect
Native results. The production SDK plugin is not used as a second reader.

Source implementations and SDK-to-public-SQL fixtures are not a completed
production support claim. Consult the current packaged Native and Java integration
results for the exact ABI and platform; earlier ABI-specific notes are historical.
Overall production qualification remains open.
A successful direct Lance Dataset test does not establish Paimon physical-file
support, and this reader work does not establish a Native physical-file writer.
