# Catalog deployment revision lifecycle

## Shipped safety boundary

Canonical write tasks bind a catalog name, incarnation ID, revision, provider and
fingerprint. The authenticated deployment endpoint now reads the immutable
`catalog_descriptor_revisions` snapshot for that exact identity. It never
substitutes the current `catalog_definitions` row. The current-row compare-and-swap
and historical insert commit in one metadata PostgreSQL transaction. V35 backfills
the **current** row of every existing catalog; revisions overwritten before V35
cannot be reconstructed and an exact request for them fails closed.

Catalog DROP deletes only the current row. DROP/re-CREATE gets a new incarnation;
old and new snapshots remain distinguishable. Historical rows are not automatically
deleted. This is an intentional capacity debt, not a claim of complete lifecycle
management. Operators should monitor total historical row count, `pg_total_relation_size`
of `catalog_descriptor_revisions`, growth per catalog/incarnation and oldest
`recorded_at`; alert on sustained growth and plan storage accordingly. Manual
deletion is unsafe until the durable task-reference protocol below exists.

The endpoint's existing shared `X-Internal-Auth` secret is **not** tenant-scoped
and the endpoint does not itself require TLS. Any Worker holding that secret can
request another tenant's descriptor; descriptor properties can also contain
connector secrets. This remains a separate P1 security debt: require mTLS
workload identity, authorize the bound task/tenant and exact descriptor identity
at the endpoint, and return secret references for Worker-side resolution rather
than plaintext values. The revision-history change does not claim to close it.

## Required lease/GC contract before enabling deletion

1. Add `catalog_revision_lease` in the same metadata authority, keyed by exact
   `(catalog_name, incarnation_id, revision, query_id, task_id, attempt_id)`.
   Store tenant, execution-owner fencing identity, task deadline, replay/recovery
   horizon, durable state (`RESERVED`, `DISPATCHED`, `TERMINAL`), and update time.
   Identity and times must be server-validated; a Worker cannot self-extend a lease.
2. Reserve the exact revision **in the same durable transaction** that commits a
   dispatchable TaskRequest/outbox record. A Coordinator must not transmit a task
   before that transaction commits. Retrying or HA replay renews the same
   reservation under the live execution anchor and exact owner fence. Retain
   references needed by write-commit reconciliation even after a task reply.
3. On durable task/query termination, mark the reservation terminal after all
   replay and write-commit paths are impossible. Use an explicit finite maximum
   deadline plus recovery SLA configured and tested across Coordinator/Worker,
   not a guessed descriptor TTL. If an owner, deadline, or journal outcome is
   unknown, retain the revision.
4. GC may delete a non-current revision only in an authority transaction that
   excludes concurrent reservation, proves there are no live or recoverable
   references, and proves the safety horizon has elapsed. Use a database/advisory
   lock or row lock with one documented lock order; bound batch size and retries.
   A lease/GC race must deterministically preserve either the snapshot or reject
   dispatch before it leaves the Coordinator.
5. Keep exact endpoint authorization, tenant validation at the Worker, and
   fingerprint checking. A missing snapshot remains a hard error; never return a
   newer revision or use provider-level defaults as a compatibility path.

Acceptance requires PostgreSQL restart/failover and race tests for reserve versus
GC, DDL update/drop/re-create during an in-flight task, task replay near the
deadline, abandoned owner takeover, and pending write-commit reconciliation.
Validate the retention horizon against the configured maximum task deadline and
recovery SLA before turning on any periodic deletion. Until then, GC stays off.
