# Canonical PostgreSQL migrations

This resource-only JAR owns the unified, previously Metadata-packaged database
migration sequence. It contains no Java implementation or provider. Metadata's
bootstrap runner consumes it at runtime; Testing Kit's `MigratedPostgres` fixture
lets domain owners test their JDBC stores without depending on Metadata runtime.

The classpath location remains `db/migration`. Existing filenames, version numbers
and SQL bytes are unchanged. Do not renumber, edit or split released migrations by
domain, and do not retain copies in consumer modules. Add schema changes as new
versions in the existing sequence and append their version, SHA-256 and classpath
path to `src/main/resources/META-INF/pangrova/schema-migrations.tsv`.

`SchemaMigrationBundleTest` in Testing Kit verifies the inventory, unique resources,
original bytes, fresh initialization, repeat migration and upgrade from V60 while
preserving history checksums and catalog data. `MetadataMigrationClasspathTest`
checks uniqueness again with Metadata's actual dependency graph. The architecture
gate enforces `resourceOnly` ownership and rejects Java sources, bytecode and
ServiceLoader descriptors in this module.

Tenant persistence uses only `tenants` and `tenant_user_memberships`. V103 records
quota presence independently of zero limits and installs current-authority mutation
gates for both tables. Tenant stores do not create schema. The older normalized
Tenant model is retired without data-copy or compatibility tooling. V104 owns all
Security policy/audit tables and authority triggers. Security providers perform no DDL;
run this bundle before constructing them. Existing migration bytes remain unchanged.

V106 owns cluster concurrency counters and exact live-query reservations. Coordination
PostgreSQL providers validate these tables but never create them. New databases use
the same canonical initialization as Server and backend tests; no legacy data mapping,
dual write or automatic reservation reset is supplied.

V107 owns deployment-scoped synchronization checkpoints and immutable completion receipts.
V108 owns projection operation receipts, committed with the projection and invalidation
under the current Catalog deployment lock. Receipt rows deliberately survive Catalog
removal so an unknown commit can be confirmed by its original operation ID. Providers
never create these tables; tests and Server use this same empty-database bundle.
Receipt retention is not yet implemented and must preserve unknown-outcome confirmation.

V109 and V110 own immutable projection/completion intents. They survive Catalog deletion;
current-deployment fencing determines obsolete-request rejection. Each workflow permits only one
pending intent per Catalog. Applied state commits with the original effect and receipt. These are
canonical empty-database initialization resources, not a historical-data conversion workflow.

V112 records the selected CDC source factory on a stream. The provider identifier is required during creation and retained across recovery.

V115 records the current federation observation epoch on the locked catalog deployment row.
