# Actual Paimon SDK Avro metadata

These four immutable objects were produced by Apache Paimon 1.4.1 using its
ordinary batch writer and the default Avro/Zstandard manifest settings. The
source tables have `id BIGINT`, `region STRING`, `label STRING`, partition by
`region`, one bucket keyed by `id`, and Parquet data files. Two commits write
`(1, east, old)`, `(2, west, NULL)`, then `(1, east, new)`. The primary-key
variant additionally keys by `(region, id)`. No metadata records were fabricated.

The two entry objects each contain two ADD records; each list object contains
one manifest reference. Native tests read all nested fields and nullable arrays.
Full SQL acceptance additionally requires the complete 20-column FilesTable contract.
Its current public SQL path is still unimplemented; format compatibility alone
must not be reported as Paimon `$files` end-to-end acceptance.

The source configuration and two commits are listed above. The runtime source
version is pinned by Maven; the capture date is 2026-10-21. The source properties, metadata
schema, entry serializers and field values were checked against that SDK's source
JAR. Field encoding follows the [Avro object container specification](https://avro.apache.org/docs/1.12.0/specification/).

| Object | SHA-256 | Bytes |
| --- | --- | --- |
| `append-manifest-entry.avro` | `115c498104cb8006deb2ada92b43b479cb9272e509e9be511d6fdb6bc7f4c6ad` | 2176 |
| `append-manifest-list.avro` | `53abe36924052d41a6f31ae98c991e76f1a5884af9baf9cca7779e73887ea314` | 1121 |
| `primary-key-manifest-entry.avro` | `35a03f5495336f550879b45e0dc95fa3457893a60c9f5990ccebac677924efb2` | 2183 |
| `primary-key-manifest-list.avro` | `9eeb0ba713111e9e83743957becc530600da4b855c7b6fae681801cbc4f65b9d` | 1122 |
