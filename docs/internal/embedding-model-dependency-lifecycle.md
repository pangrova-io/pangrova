# Embedding model dependency lifecycle: implementation contract

Status: design specification. `DROP EMBEDDING MODEL`, with or without `CASCADE`,
currently fails closed with `AI-0007`; it does not delete the model record or
rewrite generated-column definitions. This is a safety gate, not completed model
lifecycle support.

## Current authority gap

`DropEmbeddingModelTask` calls `EmbeddingModelRegistry.drop`. The JDBC registry
stores model descriptors in `pangrova_embedding_models`, but
`DdlExecutor.createTableInternal` serializes `GeneratedColumnSet` into connector
table properties before calling `ConnectorMetadata.createTable`. These are
different transaction authorities. `has_dependents` is an advisory legacy bit:
CREATE TABLE does not update it, so false cannot prove the absence of table
definitions. The connector ALTER SPI already rejects generated-column additions
because schema and generated metadata cannot currently mutate atomically.

## Durable dependency reservation

The metadata PostgreSQL authority needs a `model_dependency` table keyed by
model incarnation and table/target-column identity, with an immutable DDL
operation ID, connector catalog identity, expected table metadata revision,
definition checksum, state (`RESERVED`, `ACTIVE`, `REMOVING`, `RELEASED`),
fencing token, lease, and timestamps. A model row has `ACTIVE`/`DROPPING`
state and a monotonically increasing generation. Constraints must prevent two
live reservations for one table/target column, and reserve against the exact
model incarnation rather than a reusable model name.

CREATE TABLE containing generated columns must first, in one PostgreSQL
transaction, lock the model rows, check exact versions and `ACTIVE` state, and
insert reservations. The connector table metadata must include the operation
ID, model incarnation, and generated definition checksum in its atomic table
creation. Only after connector success may the reservation become `ACTIVE`.
An ambiguous connector outcome leaves `RESERVED` and blocks DROP until a
fenced reconciler verifies the table metadata and either activates or releases
the reservation. Failed CREATE may release only after proving the table was
not committed. DROP TABLE similarly records intent before connector mutation
and releases dependencies only after verifying the table is absent. No
last-writer-wins dependency flag or application-cache scan is authoritative.

Before enabling DROP, backfill every existing generated-column table property
into this authority and record a complete, versioned coverage watermark for
each connector catalog. A catalog that cannot be enumerated, decoded, or
reconciled keeps model DROP disabled. New DDL must use the reservation path
exclusively; there is no legacy dual-write mode.

## DROP and CASCADE behavior

For DROP without CASCADE, lock the exact model row in PostgreSQL, require a
complete coverage watermark and zero `RESERVED`/`ACTIVE`/`REMOVING`
dependencies, then tombstone/delete the model in the same transaction. The
same row lock and foreign-key fencing serialize concurrent CREATE reservations.
If any dependency or uncertain connector outcome remains, return a dependency
or unavailable error without mutation.

Cross-connector CASCADE cannot be advertised as one atomic transaction with
the current SPI. A safe resumable alternative requires an explicit durable DDL
job: (1) transactionally mark the model `DROPPING`, fence new reservations and
inference, snapshot dependent identities/revisions, and lease the job; (2) for
each table, perform an idempotent connector compare-and-set of schema and
`GeneratedColumnSet` in one catalog commit, removing only definitions bound
to the exact model incarnation while preserving existing VECTOR data; (3)
persist each verified table outcome; (4) after all dependencies and ambiguous
operations reconcile, atomically mark the model dropped and complete the job.
Worker retries need lease/fencing tokens. A crash or partial connector failure
must leave `DROPPING`, retain the model record, block new use, and resume from
durable progress; it must never silently publish a successful DROP. This saga
offers fail-closed recovery, **not globally atomic visibility**. If SQL CASCADE
requires atomic visibility across connectors, keep it disabled until a single
catalog transaction authority or a connector transaction contract exists.

## Acceptance gates

- PostgreSQL and connector integration tests cover create/drop races at each
  barrier, exact model-incarnation fencing, and a stale/coordinator takeover.
- Crash injection covers before and after reservation, ambiguous connector
  commit, each CASCADE table update, and final model deletion; restart produces
  no dangling definition or false success.
- Backfill proves complete catalog coverage; an unavailable catalog or corrupt
  generated metadata blocks DROP without relying on `has_dependents`.
- CASCADE retains existing vector values, preserves unrelated generated
  definitions, and reports durable partial/in-progress state until verified.
- Security tests prove ADMIN is checked before catalog disclosure and all
  generation/fencing checks fail closed.
