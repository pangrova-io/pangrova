# Iceberg SDK patch

The parent POM selects the separately versioned Apache Iceberg candidate
`1.11.0-pangrova-schema-candidate1`. On a clean machine, run
`.pangrova/scripts/build-iceberg-sdk-candidate.sh` before Maven. The script
downloads a pinned source archive, verifies the archive and patch checksums,
applies the tracked patch, records provenance, then builds the consumed artifacts
with Apache Iceberg's Gradle wrapper into the local Maven repository. Do not
replace a release JAR, copy SDK classes into the connector, or shadow SDK packages.

The patch targets official source commit
`6976e020b894f6a6777704df2b8c4458cb291ae9`; the source archive SHA-256 is
`9a79d359e8856290edb99b227d469ee4e7e4d1d92ac81e8488f270adca9c415f`, and the
patch SHA-256 is
`b5331c0c7404df2fc2b86e14b59c0c881fe5d32337f6fa53c6a3e50699ed0423`.

The root defect was in `JdbcCatalog`: catalog table and V1 discriminator checks
used unrestricted JDBC metadata patterns. A similarly named object or an
initialized catalog in another schema could incorrectly satisfy those checks,
leaving the actual connection schema without catalog tables. The patch scopes
metadata to the connection's catalog/schema, escapes literal names where the
driver supports it, validates exact returned names for every driver, and
propagates metadata failures rather than treating them as absence.

Verification completed against this candidate:

- Four real PostgreSQL schema-isolation regressions: 4 passed, 0 skipped.
- Upstream JDBC catalog suite: 301 passed, 0 failed, 22 unchanged upstream skips.
- Consumed SDK module suites: 9,009 passed, 0 failed/errors, 488 unchanged upstream
  skips; Checkstyle passed and the explicit-baseline Revapi reports had no API
  differences.
- Connector owning-suite: 412 passed, 0 failed/errors/skips.
- Fresh independent-process artifacts: smoke and two-Worker AI SQL, restart,
  released Native owners, authenticated audit attribution, and tenant reservation
  cleanup passed.

These results close only the JDBC schema-isolation defect and the listed
compatibility checks. The distinct Decimal/Parquet failure-atomicity issue,
resource-governance, full regression, sanitizer, and large Linux cluster gates
remain open; do not infer their completion from this SDK patch.
