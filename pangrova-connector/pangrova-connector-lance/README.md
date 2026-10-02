# Lance connector

The `lance` ServiceLoader plugin keeps table identity, exact logical columns,
schema fingerprints and visible dataset versions in PostgreSQL. Scans use pinned
canonical Native dataset splits. Writes use a connector-owned allocation and
reference-only winner metadata; no Java page sink reads or writes Lance rows.
Empty schemas are durably tenant-scoped as well. Schema creation requires an
authenticated tenant session; creating a table in an absent schema is rejected.
This connector directly exposes Lance datasets. It is not a prerequisite for
AI/vector execution or a replacement for Paimon's physical-file format plugin;
Paimon snapshots retain ownership of files produced by its `paimon-lance` plugin.

Catalog properties:

| Property | Meaning |
| --- | --- |
| `jdbc.url` | Required PostgreSQL JDBC URL for the durable catalog directory. |
| `jdbc.user` | PostgreSQL user, when not supplied by the JDBC URL. |
| `jdbc.password` | PostgreSQL password; use a deployment secret reference. |
| `warehouse` | Required absolute local path, canonical `file:///` URI, or normalized `s3:` warehouse URI. `file:/` is rejected without rewriting. |
| `native.storage.s3.endpoint` | Required explicit `http:`/`https:` endpoint for S3 warehouses. |
| `native.storage.s3.region` | Required S3 region. |
| `native.storage.s3.path-style-access` | Required `true`/`false`; use `true` for path-style deployments. |
| `native.storage.s3.tls-enabled` | Required `true`/`false`; must agree with endpoint scheme. |
| `native.storage.s3.access-key` | Required catalog-scoped credential; use a Worker deployment secret reference. |
| `native.storage.s3.secret-key` | Required catalog-scoped credential; use a Worker deployment secret reference. |
| `native.storage.s3.security-token` | Optional catalog-scoped session token. |

Workers resolve deployment secrets after exact catalog incarnation, revision,
configuration fingerprint and tenant authorization. Only the immediate local
JNI command/task copy receives typed object-store settings. HTTP/dispatch input
carrying these settings is rejected; Rust does not use ambient AWS credentials
or rewrite the dataset URI. Dataset locations must be normalized descendants of
the exact warehouse path, not string prefixes such as `/foo` matching `/foobar`.

Published scans enumerate canonical manifest partitions through the generic
`DATASET_SCAN_PARTITIONS` metadata command. Each page contains at most 256
descriptors, exact version, persisted-manifest SHA-256 and physical schema SHA-256.
The format-owned `partition_id` is an immutable physical manifest ID, never a byte
offset. Continuation pages must retain the same identities and cover the manifest
without duplicates or omissions. Each Native reader validates and scans exactly
one listed physical partition; it cannot scan the full dataset for every split.
The sole Lance C FFI ABI is version 6 and requires Worker-local storage authority
and a canonical partition descriptor for reads.

The Coordinator supplies the generic Native dataset control port. Schema fences
come from the Worker Native normalized physical schema, not a Java schema hash.
The port records a confirmed publication identity in the write journal before
returning `COMMITTED`; only then does this connector publish the PostgreSQL
directory with an exact table-incarnation/base-version/revision CAS. Replaying the
same fingerprint and publication identity is idempotent. `UNKNOWN` and
`NOT_COMMITTED` do not advance the directory.

An empty table has dataset version zero. Its first `INSERT` or `OVERWRITE` uses a
physical `CREATE` allocation while preserving the SQL mode. Worker plugin
creation and handle decoding do not open PostgreSQL or acquire Java page sinks.

Connector unit and PostgreSQL tests establish local contract and catalog behavior.
Production acceptance additionally requires the real public SQL → planner →
canonical v2 → Worker Native fragment → single fenced publication chain and
restart/recovery tests; component tests alone are not that evidence.
