# Iceberg REST publication fence: protocol comparison and acceptance contract

Status: design finding, not an implemented or accepted REST write path. The
`Gravitino` Iceberg REST empty-table lifecycle has run, but Pangrova's nonempty
declaration-fenced write remains fail closed at `IcebergDeclarationCommit`.

## Why the existing REST requirement set is insufficient

Pangrova freezes the table UUID, location, format version, current schema JSON,
partition-spec JSON, and declaration-relevant properties when allocating a
write. Immediately before each SDK commit attempt,
`IcebergDeclarationTableOperations` checks that whole declaration and delegates
to a catalog whose metadata-pointer compare-and-swap (CAS) has been verified.
The check and pointer CAS must form one atomic commit attempt; a client-side
refresh followed by a separate REST commit leaves an ALTER race between them.

The Iceberg REST `CommitTableRequest` requirements can assert the table UUID,
snapshot ref, current schema ID, default spec ID, default sort order ID, and
last assigned IDs. They do **not** include an assertion for the entire prior
metadata location, table location, arbitrary properties, or complete
declaration fingerprint. A concurrent change to a property used by Pangrova's
write declaration can therefore pass the standard requirements if the
snapshot/schema/spec/order IDs stay unchanged. Snapshot-only checks and a
preflight GET cannot close this gap. The REST endpoint can still implement an
atomic Iceberg commit; that is a different guarantee from Pangrova's stronger
allocation-time declaration fence.

The [Iceberg table format](https://iceberg.apache.org/spec/) describes the
metastore commit as a compare-and-swap of the entire metadata pointer, while
the [Iceberg REST OpenAPI requirement types](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
enumerate the narrower wire assertions. The [Gravitino Iceberg REST service](https://gravitino.apache.org/docs/1.2.0/iceberg-rest-service/)
is a suitable server to test, but its availability alone is not evidence that
it offers the additional declaration fence. A current Gravitino proposal for
atomic in-place registration explicitly uses `TableOperations.commit(base,
target)` for the pointer swap; registration is a separate operation and must
not be treated as an implicit Pangrova write fence
([proposal](https://github.com/apache/gravitino/issues/11059)).

## Candidate production contract

The preferred extension is a server-side, authenticated conditional commit
that reads the latest metadata **inside the same transaction/linearizable
critical section** used to publish the new metadata pointer. Its request must
carry an expected prior metadata pointer (or a collision-resistant full
declaration digest with a monotonic version that every metadata mutation
updates). It must reject a changed UUID, location, format version, schema,
partition spec, or declaration property even if the current snapshot ref is
unchanged. The server must perform the conditional check and Iceberg metadata
publication atomically against its authoritative backend. A lock held only in
Pangrova does not fence Spark, Trino, or other direct REST writers.

The capability must be explicit and bound to the exact catalog deployment.
`IcebergDeclarationCommit.requireSupported` can admit a REST catalog only after
the server advertises a versioned capability and a real conformance test proves
its race behavior. It must not admit arbitrary `RESTCatalog` instances by class
name or by optimistic client refresh. If the extension is unavailable, keep
nonempty writes fail closed; reads and supported empty-table operations remain
independent.

The current local `HadoopCatalog` allowance supports development fixtures. The
[Iceberg format](https://iceberg.apache.org/spec/#file-system-tables) now
explicitly calls filesystem-table atomic rename deprecated and unsafe on local
filesystems and object stores. Its local positive tests do
not certify a production shared-warehouse commit backend; production catalog
admission must continue to require a verified metastore pointer CAS.

## Required evidence before enabling REST writes

1. Real Gravitino-backed, nonempty INSERT/SELECT/DELETE and the corresponding
   Iceberg SDK read of the committed snapshot; capture the server/version and
   catalog configuration.
2. Deterministic two-client interleaving: pause the writer immediately before
   the server commit; a different REST client changes a declaration property,
   location, schema, or spec without advancing the snapshot. Resume the writer;
   it must receive a conflict and publish no data snapshot.
3. A concurrent data-only append may advance the snapshot and follow the
   Iceberg SDK's safe retry semantics, provided the frozen declaration is still
   equal. Verify no duplicate publication or lost append.
4. Unknown outcome (lost commit acknowledgement): read the authoritative
   metadata and commit marker, distinguish already-published from not-published,
   and never issue a second visible publication for the same attempt.
5. Drop/recreate, credential or catalog deployment change, and a stale owner
   must all fail the frozen identity/fencing checks before publication.

These are acceptance criteria, not substitute tests. The existing real
Gravitino nonempty cases still fail closed and must be rerun only after the
server capability and client binding are implemented.
