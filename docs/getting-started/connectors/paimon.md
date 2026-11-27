---
title: "Paimon"
description: "Apache Paimon connector configuration"
---

# Paimon

Configure the Paimon connector for streaming lakehouse tables.

Distributed commit retries remain inside the Paimon SDK, which tracks snapshot
commit identity. If that operation throws, the connector does not restart it
based on words such as "conflict" in the exception message: a lost publication
response may still represent a committed snapshot. Recovery must reconcile the
durable commit fingerprint before replay; an exception alone does not prove
that staged files are safe to delete.


## Paimon Connector

Apache Paimon provides streaming lakehouse capabilities with real-time data ingestion.

### Catalog Configuration

The YAML is a reference format; use SQL registration for the runtime catalog.
Automatic startup registration remains planned.

```yaml
# etc/catalog/my-paimon.yml
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

For a Paimon REST catalog, set `metastore=rest`, `uri`, `warehouse`, and the
Paimon 1.4 authentication options (`token.provider` and its credentials) in the
catalog configuration. Keep credentials in the deployment's secret store. The
connector sends JSON request bodies with `Content-Type: application/json`
unless that header is explicitly configured. The REST service must return its
resource `prefix` in `/v1/config`; the Paimon 1.4 client requires it when
resolving table resources. Validate that the chosen service supports the
snapshot and commit endpoints before enabling writes. The Apache
`paimon-rust` filesystem-backed REST server is useful for local integration
tests, but its current commit path does not enforce table-UUID concurrency
fencing and is not a production write catalog. See the
[Paimon REST catalog specification](https://paimon.apache.org/docs/1.4/concepts/rest/overview/)
and the [Apache test server](https://github.com/apache/paimon-rust/blob/v0.4.0/crates/paimon-rest-server/README.md).

### Incremental snapshot validation

Append-only capability checks validate that both snapshot endpoints exist before
inspecting the interval. Equal IDs describe an empty interval only for an existing
table and snapshot. Missing or expired endpoints, non-positive IDs, and reversed
intervals fail explicitly; they cannot be accepted as an empty change set. Paimon
snapshot IDs advance sequentially, while retained history may lose older snapshots
through [snapshot expiration](https://paimon.apache.org/docs/1.4/maintenance/manage-snapshots/).

### Split planning I/O

All physical Paimon scan sources in one Coordinator stage share the stage's
monotonic planning deadline and a 256 MiB encoded metadata read allowance.
Each source has its own one-million manifest-open ceiling. Standalone split
enumeration starts with its own allowance. The scan reserves bytes before
calling the underlying reader. Unused bytes return to the stage when that read
exits. The allowance covers bytes delivered by file I/O to the SDK during scan
planning. Manifest cache hits do not open files. Exceeding a limit rejects the
plan.

When a plan exits after cancellation or failure, its metadata view rejects
new opens. The Coordinator's query operation stays active until previously
admitted backend calls and streams close. Paimon planning uses the existing
SDK scan and its worker pool.

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
Ordinary bound split planning passes the verified snapshot to a query-local
SDK scan view. Historical scans take their schema ID from that object.
The Coordinator reads the source snapshot object again before publishing
the plan, so expiry or replacement during planning rejects the scan.

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
