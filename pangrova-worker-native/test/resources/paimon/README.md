# Actual Paimon SDK Avro metadata

These four immutable objects were produced by Apache Paimon 1.4.1 using its
ordinary batch writer and the default Avro/Zstandard manifest settings. The
source tables have `id BIGINT`, `region STRING`, `label STRING`, partition by
`region`, one bucket keyed by `id`, and Parquet data files. Two commits write
`(1, east, old)`, `(2, west, NULL)`, then `(1, east, new)`. The primary-key
variant additionally keys by `(region, id)`. No metadata records were fabricated.

The two entry objects each contain two ADD records; each list object contains
one manifest reference. Native tests read all nested fields and nullable arrays.
The ordinary append/primary-key SQL paths now compare the complete twenty
columns with the SDK across commits. The repository validation inventory records
the exact affected-scope results; these fixtures alone do not prove the broader
type, filesystem or final production acceptance matrices.

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

The `*-files-oracle.json` files pair the original entry objects with their
original snapshot JSON, schema JSON, complete SDK physical-identity SHA-256s,
and all twenty `FilesTable` output values for those exact physical files.
Only output `file_path` prefixes are replaced with `file:/fixture/`; the data
file names, original metadata bytes, statistics and creation timestamps are
unchanged. Snapshot URIs are supplied by each test's temporary directory.
Creation timestamps use `Timestamp.getMillisecond()` in the golden values.
Native reads the original Avro object; the golden result values are never
included in its read context. The identity framing is big-endian length-prefixed
UTF-8/bytes following the complete SDK `FileEntry.Identifier` fields.

- `append-files-oracle.json`: SHA-256 `08caefd64e7dd44cf45bffe432ed44d22cb917cb3750ec078a010dbdeb776183`.

- `primary-key-files-oracle.json`: SHA-256 `53c9f18c446874eaeb56fd8422814ac23aa9f9cc564e71040faa19085ac2d12a`.

`projected-manifest-entry.avro` is a controlled metadata edge fixture written
by the same SDK's `ManifestFile.write`, derived from one original primary-key
entry through `DataFileMetaSerializer.toRow/fromRow`. It sets level 3,
`extraFiles=[companion.index]`, embedded index bytes `01 02 ff`, source COMPACT,
an external `file:/fixture/external/` path, first row ID 900, a null delete count,
and `writeCols=[id,label]`. The SDK-computed complete file identifier is frozen
in `projected-files-oracle.json`; its changed result values come from the
round-tripped SDK `DataFileMeta` getters. This is a serializer/Native boundary
fixture, not evidence of a committed projected-write data file or of compaction
SQL. Native verifies every non-null output array element, not only array size.

- `projected-manifest-entry.avro`: SHA-256 `8eebabd2204542c0370658d4ba2dff7a8b16fb1978dcc5753137671d10f35f4a`.

- `projected-files-oracle.json`: SHA-256 `b96e28f1d276b43179546f695403963bbeb1778538776a99301453c874f418c2`.
