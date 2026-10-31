# Coordination JDBC

This module implements `DistributedLease`, `CoordinatorRegistry`, and
`WorkerRegistry` over a caller-supplied JDBC `DataSource`. The implementations
depend on Coordination API and Common, with Jackson for registry JSON and SLF4J
for logging. Neither production nor test dependencies include Coordination Core
or Metadata runtime or their test fixtures.

The process assembly owns the data source and schema migration. These providers
close borrowed connections and statements; they do not close the data source or
initialize the schema. Canonical migrations remain in `pangrova-schema-migrations`.
The existing `distributed_lease`, `coordinator_registry`, and `worker_registry`
tables and their stored values are unchanged.

Lease acquisition, renewal, and release retain their database-issued fencing
epochs and PostgreSQL live-clock checks after row locking and before commit.
Database failures throw `CoordinationStorageException` with the existing
`METADATA_METASTORE_ERROR` client code and original cause, rather than becoming
contention or absence. Registry error behavior remains unchanged.

Tests use Testing Kit's singleton PostgreSQL and isolated schemas, including the
canonical migration bundle where required. The owner suite covers lock waits,
expiry, takeover, registration rollback, inventory recovery, and schema upgrades.
`JdbcCoordinationClasspathIsolationTest` also checks the actual test classpath has
no Coordination Core or Metadata implementation classes.

Run the complete owner suite and static analysis from the repository root:

```sh
mvn -B -pl pangrova-coordination/pangrova-coordination-jdbc -Pfull-regression,static-analysis verify
```
