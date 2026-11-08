# Iceberg write-failure evaluation patches

These five unified patches reproduce the isolated Iceberg 1.11.0 / Parquet 1.17.1
write-failure candidate. Status: **SOURCE_READY; production dependency not switched;
formal release acceptance BLOCKED**. They are retained for review and are **not**
applied by the current SDK build, included in production artifacts, or considered
release-ready.

Direct upgrade to official Iceberg 1.12.0 does not fix the failure (it still depends
on Parquet 1.17.1), so the minimum 1.11.0 + Parquet Hadoop 1.17.1 backport is the
review baseline. The candidate was evaluated against the exact pinned source archives
and an isolated classpath; the patches preserve the 1.11 constructor signatures and
row-data behavior. Before use in any build, verify the pinned source commit/archive
hashes and each patch checksum, apply to a fresh source tree, then run the upstream
official build and the full SDK/connector/Worker/E2E acceptance matrix. The candidate
has not passed that release gate.

Known open boundaries: the main-source `IcebergDeleteWriter` position/equality delete
builders still lack `finally`/try-with-resources around write failures; ORC/Avro,
other FileWriters, remote object stores, encryption, and every allocator boundary are
not covered by this candidate. Cleanup-only failures never restore a writer's
publication eligibility, and a real mid-write I/O failure requires deleting the whole
uncommitted file because the public `PositionOutputStream` has no safe rollback.

Patch SHA-256 values:

| Patch | SHA-256 |
| --- | --- |
| `DataWriter.java.patch` | `91fed6afedb7926a87f6d9483bca061d47560180ce7acf838cf625fb8d4bc5f8` |
| `EqualityDeleteWriter.java.patch` | `890f5a4f68cf4ea0f3f29d891acdb214a77daf7cdbe403808c1c22b7f2c574c5` |
| `PositionDeleteWriter.java.patch` | `d5f45ca26dd75ff819c33579bb2a7b2e83c6a391870f331b837af91866a56295` |
| `ParquetWriter.java.patch` | `652538dd2e0c01fd89191642d79bb0bb3bb5682515fb374d8de905b6cade0091` |
| `ParquetFileWriter.java.patch` | `b0f1b7a90d84ab1e661f6b059426edcc425aba98a3a5f2d18abacc2201ee04c2` |
