# Worker registration HTTP envelope

The Worker registers through `POST /v1/workers/register`. Its canonical Native capability inventory is Base64 encoded in the JSON request. The inventory codec accepts up to 4 MiB of encoded data. The generic Coordinator HTTP limit is 1 MiB, so applying that generic limit to registration rejects a valid inventory before the Worker can join.

Registration has an inclusive 5,242,906-byte transport bound: 1 MiB for its other JSON fields, 4 MiB for the encoded inventory, and 26 bytes for the inventory JSON property and separator. The Worker and Coordinator both check the non-inventory fields against the same 1 MiB bound using their actual Jackson serialization. The Coordinator applies the larger transport bound only to the registration `POST`; heartbeat and other routes retain the generic HTTP limit. Declared and streamed body lengths are both enforced.

The transport limit permits a valid canonical inventory; it does not authorize the model or a query. The inventory codec still validates the canonical v2 descriptor and individual capability contracts, and the registration resource still requires internal service identity before accepting a Worker.

Every registration supplies `worker_execution_identity`, with canonical UUIDs
for `admission_domain_id` and `incarnation_id`. The domain identifies the Worker's
persistent admission store; the incarnation identifies its physical execution
lifetime. Coordinator snapshots persist both values and preserve them through
heartbeats and leader recovery. V82 leaves older registration rows without an
identity undispatchable until the Worker registers again. A heartbeat cannot
upgrade such a row or invent physical ownership evidence.

The Coordinator binds the selected Worker identity into `TaskAttemptId` before
access binding, frozen COPY input admission, and the durable dispatch intent.
An already-bound task cannot be redirected to a different incarnation even at
the same host and port. Both ordinary partition dispatch and direct synchronous
or asynchronous HTTP dispatch enforce that check. Recovered task intents retain
the originally selected identity rather than substituting the current registry
entry after a Worker restart.

Shutdown stops the CDC consumer, heartbeat scheduler, HTTP listener, and task
owners before sending the Coordinator's deregistration request. If an
independent owner does not close, or the Coordinator does not acknowledge
deregistration, the registration client and dependent Native function
resources remain available for an explicit cleanup retry. A caller interrupted
before scheduler shutdown does not irreversibly stop that scheduler. After a
valid deregistration acknowledgement, the Worker closes the registration
client, Native function owners, and remaining runtime resources in dependency
order.

Task submission validates the canonical task request before admitting it.
Malformed checksums and incomplete resource grants return a structured HTTP
400 response; failures after validation that indicate a Worker fault retain
HTTP 500. Fencing and capacity failures retain their dedicated response codes.

The Worker opens `worker.admission.state-directory` (default
`data/worker-admission`) before starting Native execution. This directory must
be durable local storage, unique to that Worker, and retained across restarts.
Do not place it on a shared/network filesystem, clone it to another running
node, or delete it while recovery still refers to its domain. A new directory
creates a different domain and cannot prove retirement in the old domain.

A process-exclusive file lock protects the incarnation ledger. Each fresh
incarnation is appended with a chained SHA-256 checksum and forced to storage
before its identity is advertised. Acquiring the same lock proves that prior
recorded incarnations have either terminated or completed the full shutdown
join. Shutdown releases the lock only after all dependent Native, task and
source-preparation owners are closed. A failed join retains the lock. The same
incarnation cannot initialize a second task-admission registry.

Recovery truncates an incomplete final startup record, which was never
published to admission callers, and preserves earlier complete records. A bad
checksum in a complete record or header fails startup; elapsed time, a new
registration and a missing task never repair that proof. The state directory
must be recovered from intact storage or the former process must be physically
fenced through a separately verified operator procedure.

A seal for a known retired target returns the distinct
`WORKER_INCARNATION_RETIRED` cancellation receipt, even when the old task was
never delivered or its admission deadline has expired. That receipt carries the
original task attempt and only proves physical exit; it cannot publish task
results or infer a successful write. Unknown domains and incarnations fail
closed. Current-incarnation seals retain the ordinary exact admission and
Native drain checks.

The locking lifetime follows the [JDK FileLock contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/channels/FileLock.html).
The ledger is forced before authority is exposed, following the durability
ordering described in [PostgreSQL's WAL introduction](https://www.postgresql.org/docs/16/wal-intro.html).

Native status serialization and task-scoped control commands use the complete
admitted attempt retained in the canonical execution callback context. They do
not reconstruct a subset of its fields; Worker incarnation identity therefore
survives JNI execution, terminal reports and command fencing unchanged.

Exchange transport keeps both Worker UUIDs in its runtime-neutral attempt and
preserves them in direct Native commands and remote Shuffle RPCs. Shuffle schema
V7 stores the complete admitted attempt in the producer fence and committed
winner, before allowing object writes or accepting an empty producer commit.
Retries must match that durable attempt exactly. Reads return its original
identity rather than reconstructing fields from the current Coordinator owner.

V7 leaves preexisting attempt records without a canonical identity explicitly
NULL. Data replay and reads reject those records; lifecycle cleanup can still
remove them. Drain active shuffles before this protocol/schema upgrade. Do not
backfill an old attempt from a new request or current Worker registration: neither
establishes which process originally owned the work.
