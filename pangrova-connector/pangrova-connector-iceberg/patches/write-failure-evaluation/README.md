# Iceberg write-failure evaluation patches

These five unified patches reproduce the isolated Iceberg 1.11.0 / Parquet 1.17.1
write-failure candidate documented in
[`docs/internal/iceberg-sdk-write-failure-repair-plan.md`](../../../../docs/internal/iceberg-sdk-write-failure-repair-plan.md).
They are retained for review and are **not** applied by the current SDK build,
included in production artifacts, or considered release-ready.

They were evaluated against the exact pinned source archives and isolated
classpath described in that plan. Before use in any build, verify the pinned
source commit/archive hashes and each patch checksum, apply to a fresh source
tree, then run the upstream official build and full SDK/connector/Worker/E2E
acceptance matrix. The candidate has not passed that release gate.

Patch SHA-256 values:

| Patch | SHA-256 |
| --- | --- |
| `DataWriter.java.patch` | `91fed6afedb7926a87f6d9483bca061d47560180ce7acf838cf625fb8d4bc5f8` |
| `EqualityDeleteWriter.java.patch` | `890f5a4f68cf4ea0f3f29d891acdb214a77daf7cdbe403808c1c22b7f2c574c5` |
| `PositionDeleteWriter.java.patch` | `d5f45ca26dd75ff819c33579bb2a7b2e83c6a391870f331b837af91866a56295` |
| `ParquetWriter.java.patch` | `652538dd2e0c01fd89191642d79bb0bb3bb5682515fb374d8de905b6cade0091` |
| `ParquetFileWriter.java.patch` | `b0f1b7a90d84ab1e661f6b059426edcc425aba98a3a5f2d18abacc2201ee04c2` |
