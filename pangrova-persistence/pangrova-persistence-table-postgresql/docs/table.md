# Table persistence

Table identity, lifecycle, maintenance, statistics and policy values belong to
`pangrova-table-api`. The four storage ports and `TableControlException` belong to
`pangrova-persistence-table-api`, under `org.pangrova.persistence.table.api`.
`pangrova-table-core` owns domain orchestration over these contracts.

PostgreSQL implementations live in `pangrova-persistence-table-postgresql`, under
`org.pangrova.persistence.table.postgresql`. They borrow an externally owned
DataSource, close their operation connections, and never create pools or schemas.
Canonical initialization belongs exclusively to `pangrova-schema-migrations`.
Control revision, structured name reservations, invalidation and version observation
remain in one transaction. Stale revisions report `TABLE_CONTROL_STALE_WRITE`.
Observation history is ordered by successful revision acceptance for the same table,
newest first, independently of caller timestamps. Clock rollback and equal timestamps
must not reorder committed version transitions. Non-positive limits return no rows.

Governance binding upsert is atomic by `(tableId, bindingType, bindingRef)`.
Updates preserve the winning creation's bindingId/creator/time and replace config
and update time. SQL RETURNING produces the exact stored receipt before commit;
the caller receives it only after commit succeeds. Save, delete and deleteAll failures attempt rollback
and retain cleanup failures as suppressed exceptions on the primary failure. The existing authorization mutation lock remains
inside that transaction. The caller-connection `list` participant never commits,
rolls back or closes its caller's connection. Cross-domain transaction composition
still needs the later workflow extraction; the PostgreSQL Security gate dependency
is explicit and has not been replaced by an allow-all implementation.

`pangrova-persistence-table-memory` provides the existing process-local counterparts.
Atomic compute preserves the same binding creation identity within an instance.
These stores do not survive restart or provide shared multi-process authority.
Metadata/Server assembly explicitly selects the backend; Table Core has no backend
implementation dependency. Enforced deployments continue to require durable state.

Provider isolation tests use the real owner classpath; contract tests use only
contract artifacts. PostgreSQL tests initialize disposable schemas through the
canonical migration fixture. Backend tests accompany the providers, and domain/SQL
integration tests remain with their consumers.

```sh
mvn -B -pl :pangrova-persistence-table-api,:pangrova-persistence-table-postgresql,:pangrova-persistence-table-memory,:pangrova-table-core -am -Pfull-regression,static-analysis verify
```
