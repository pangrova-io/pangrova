# Iceberg delete regression fixture

`iceberg-equality-id-41.parquet` is the unmodified output of Apache Iceberg's
`Parquet.writeDeletes(...).forTable(table).withSpec(table.spec())`
`.createWriterFunc(GenericParquetWriter::create).equalityFieldIds(new int[] {1})`
`.rowSchema(schema).buildEqualityWriter()` with one record: optional BIGINT `id = 41`
(field ID 1). `TableControlLifecycleE2ETest` creates the same fixture at runtime.

The SDK file reproduced a worker JVM SIGSEGV in `readEqualityKeys`: the DWIO
reader returned an encoded child, but the code dereferenced `asFlatVector()`.
The native regression reads these actual SDK bytes. This test does not qualify
production equality-delete semantics; capability admission remains disabled.

## Iceberg v3 deletion vectors

`iceberg-sdk-*-dv.puffin` files are produced by the unmodified pinned Iceberg
SDK using `GenerateIcebergDvFixtures.java`. Compile it with the SDK runtime
classpath and run `org.apache.iceberg.deletes.GenerateIcebergDvFixtures` with
an empty output directory. All nonempty files use `BaseDVFileWriter`; the empty
case uses the same SDK bitmap serializer and `PuffinWriter` directly.

The multiple-blob fixture references `s3://fixture/a.parquet` (position 1) and
`s3://fixture/b.parquet` (positions 2 and 4294967303). Their blobs begin at
byte offsets 4 and 46, with lengths 42 and 64. This catches selection of another
data file's vector and truncation of 64-bit positions. Other files retain the
position sets in the generator. Tests corrupt copies of these bytes to verify
that invalid length, CRC and footer flags cannot produce unfiltered rows.

The previous synthetic fixtures used an incorrect `PUF1` container and raw
32-bit Roaring payload, which neither the real SDK writer nor reader accepts.
The SDK files use `PFA1`, a length-prefixed 64-bit portable Roaring map, and
IEEE CRC-32. HTTP SQL acceptance also creates a new multi-blob SDK container
at runtime and verifies both projected rows and `COUNT(*)`.
