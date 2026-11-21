# Iceberg SDK patch

The parent POM selects the separately versioned Apache Iceberg candidate
`1.11.0-pangrova-schema-candidate4`. On a clean machine, run
`.pangrova/scripts/build-iceberg-sdk-candidate.sh` before Maven. The script
downloads a pinned source archive, verifies the archive and patch checksums,
applies both tracked patches, records provenance, tests the fixed-buffer writer,
then builds the consumed artifacts
with pinned Gradle 8.14.4 into the local Maven repository. Do not
replace a release JAR, copy SDK classes into the connector, or shadow SDK packages.

The patch targets official source commit
`6976e020b894f6a6777704df2b8c4458cb291ae9`; the source archive SHA-256 is
`9a79d359e8856290edb99b227d469ee4e7e4d1d92ac81e8488f270adca9c415f`, and the
patch SHA-256 is
`b5331c0c7404df2fc2b86e14b59c0c881fe5d32337f6fa53c6a3e50699ed0423`.

The JDBC root defect was in `JdbcCatalog`: catalog table and V1 discriminator checks
used unrestricted JDBC metadata patterns. A similarly named object or an
initialized catalog in another schema could incorrectly satisfy those checks,
leaving the actual connection schema without catalog tables. The patch scopes
metadata to the connection's catalog/schema, escapes literal names where the
driver supports it, validates exact returned names for every driver, and
propagates metadata failures rather than treating them as absence.

Historical verification against `schema-candidate1` (the JDBC patch is unchanged):

- Four real PostgreSQL schema-isolation regressions: 4 passed, 0 skipped.
- Upstream JDBC catalog suite: 301 passed, 0 failed, 22 unchanged upstream skips.
- Consumed SDK module suites: 9,009 passed, 0 failed/errors, 488 unchanged upstream
  skips; Checkstyle passed and the explicit-baseline Revapi reports had no API
  differences.
- Connector owning-suite: 412 passed, 0 failed/errors/skips.
- Fresh independent-process artifacts: smoke and two-Worker AI SQL, restart,
  released Native owners, authenticated audit attribution, and tenant reservation
  cleanup passed.

## Avro fixed-buffer encoding

`iceberg-avro-fixed-buffer.patch` corrects the same pinned upstream source's
`ValueWriters.FixedByteBufferWriter` and `InternalReader`: fixed data must use
`writeFixed`/`readFixed`, without the byte-string length prefix produced by
`writeBytes`. The input is duplicated
to preserve its position and limit for every encoder implementation. Its SHA-256
is `f3dfc99a1233a4092b3b79413a23660da6510c04218efae6ae6513ae09ba8243`.
The build records both patch hashes in the candidate's `iceberg-build.properties`.

The patch includes four writer regressions for heap, read-only, sliced, and direct
buffers. They decode with Avro's own fixed decoder, verify the following integer
is still aligned, and check the input position/limit. All four fail on the pinned
original writer and pass with the fix. A fifth regression reads a standard Avro
writer's fixed record through `InternalReader` and verifies that projected-away
fixed fields use fixed-width skipping. The
writer-only intermediate candidate failed eight broader Avro regressions because
the reader independently used `readBytes`; that candidate is not selected.
The complete Avro owner run then passed 226 cases with 56 unchanged upstream
skips, zero failures/errors; both upstream core Checkstyle tasks passed.
The selected `schema-candidate4` passed the checksum-verified clean build and
publication of all six consumed artifacts. The full Java reactor installed with
tests skipped. The functional candidate3 passed 443 connector tests and static
analysis, plus the two actual v2/v3 fixed-partition SQL/JNI cases. The final
candidate4 changes only patch formatting and build provenance: all 3,360 runtime
classes across the six SDK JARs are byte-identical, so those checks are reused.
These are rolling checks,
not the final full project regression.
The separate Native reader still rejects malformed fixed encodings; it does not
recognize a second private format or retry decoding as bytes.

References: [Avro fixed encoding](https://avro.apache.org/docs/1.12.0/specification/#fixed-1),
[pinned Iceberg writer](https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/core/src/main/java/org/apache/iceberg/avro/ValueWriters.java).

These results close only the named SDK defects and the listed
compatibility checks. The distinct Decimal/Parquet failure-atomicity issue,
resource-governance, full regression, sanitizer, and large Linux cluster gates
remain open; do not infer their completion from this SDK patch.
