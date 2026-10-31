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
