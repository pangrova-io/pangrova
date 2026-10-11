# Native shuffle registration ownership

Status: SOURCE_READY; validation pending, not accepted. This specification was reviewed
against the 2026-10-07 worktree. Only matching rebuilt Native, ASan, packaged JNI
and actual SQL/distributed results can establish acceptance.

## Reproduced failure and source evidence

The full Worker run failed at `NativeShuffleTransportTest` line 41 when the same
actual executor started its shuffle server twice. The response contained
`shuffle handle is already registered: 10`; the preserved log is
`/tmp/pangrova-production-20261007-worker-full-gates-r2.log`, lines 805-811.
`ShuffleTransport.startServer` did not document replay semantics. Its production
caller, `TaskLifecycleManager.executeTask`, starts the producer before the Native
driver, once per created executor. The existing test requires exact active replay.

Four separate source defects accompany the failure:

- `FlightShuffleServer.cpp:655-664` acquires another real demuxer/handle budget
  before rejecting every duplicate, including identical active requests.
- `FlightShuffleServer.cpp:666-672` publishes the route before the drain thread
  has started.
- `FlightShuffleServer.cpp:706-724` removes the active route before quiesce and
  republishes it as lingering later, permitting a second consumer in that gap.
- `NativeCommandDispatcher.cpp:662-666` registers and obtains the port in separate
  registry operations, allowing shutdown/restart between them.

`JniBridge.cpp:556-558` additionally cleans shuffle before releasing the executor;
`HandleRegistry.acquire` does not seal already-acquired START commands. A trailing
`isExecutionReleased` check does not close this window.

## Atomic interface and ownership

Keep the existing HandleRegistry and FlightServerRegistry as the only registries.
There is no Java cache, new receipt, private protocol, budget increase or fallback.

TaskExecutor owns a task-local START control lease/seal and one actual
`shuffle::ShuffleProducerRegistration`. The server's original routing map and
TaskExecutor share the same registration object. The domain registration owns
the immutable handle, borrowed queue identity, query/stage, partition count,
plan epoch, attempt, fencing token and actual demuxer/reservation. Executor must
not depend on JNI command infrastructure.

The registration states are STARTING, ACTIVE, DRAINING, LINGERING, CLOSE_FAILED and CLOSED;
cleanup failure retains the actual owner for retry and forbids START replay.
Exact ACTIVE replay requires equality of every identity field and queue/count;
it creates no demuxer or budget reservation. Exact STARTING replay waits for the
same real startup outcome. Conflicts, sealed/cancelled owners, DRAINING and
LINGERING refuse new admission. Existing signed readers retain their original
identity and can drain clean FINISHED/LINGERING producers, including empty
partitions. Failed/aborted production is never translated into successful EOF.

The single routing map transitions the same owner through draining and linger;
there is no erase/quiesce/reinsert gap. Closed entries are reclaimed in that map.
Actual readers continue to charge the original reservations while they own them.

FlightServerRegistry returns the registration, service lifetime lease and port
from the same actual service owner. There is no second ensure/port lookup.
Service initialization and shutdown have explicit STARTING/READY/CLOSING state;
shutdown seals new borrowing, waits for actual operations and stops outside locks.
Failed stop retains the owner. Explicit successful runtime initialization opens
the next cohort; an ensure call cannot reopen a sealed old runtime.
The returned lease is move-constructible but not assignable: service destruction
must never precede relinquishing that same object's old control lease. HMAC
rotation application is serialized separately from registry metadata; startup
reconciles the actual verifier/signing snapshot against the current revision
before publication, so unlocked network initialization cannot publish stale keys.

## Exit order and lock discipline

Every release seals START, waits for admitted START operations, closes the actual
registration gracefully, proves the demuxer no longer accesses its borrowed
queue, then performs existing driver join/output drain/driver release. JNI
produces the original receipt only after real release; HandleRegistry erases
only after receipt preparation. A failure retains exact handle and sealed owner
for retry. Remove the JNI's separate early cleanup.

Cancellation, canonical CANCEL_ATTEMPT, deadline failure, notifyTaskFailed,
destruction and destroyAll must use the same task-owned seal. Preserve the
existing committed-producer winner rule: a failed abort cannot discard its
successful output or legal linger. Serialize actual release attempts, and move
the deadline thread to its unique join owner under metadata protection; no two
callers may join it, and a caller must not join the watchdog while holding a
control lease that the watchdog needs to drain. Destructor cleanup is a safety
net, not evidence of successful resource drain.
Execution start also borrows the task control gate until asynchronous submission
finishes, preventing teardown from joining/releasing a driver still being started.

No global lock is held for network start/Shutdown, disk access, demuxer quiesce,
thread join, waiting for another owner, or resource destruction. Obtain and
release locks in successive scopes: HandleRegistry snapshot, task control lease,
service owner, route owner, then demuxer/store. No lower layer acquires a higher
layer lock. Condition-variable waits release their own metadata mutex and hold
no other lock. Move discarded owners/resources out before destroying them.
HMAC update order is rotation mutex, short registry metadata snapshot, unlock,
then actual server verifier metadata; no path takes the rotation mutex from the
server. Startup checks the revision in a separate registry scope.

## Implementation closure and acceptance

The atomic source closure comprises TaskExecutor, the shuffle control/registration
domain owners, FlightShuffleServer routing and stop/eviction/readers,
FlightServerRegistry service lifecycle, NativeCommandDispatcher START/STOP,
JniBridge release/runtime readiness, RuntimeInitializer initialization/exit,
and the ShuffleTransport contract. Existing HandleRegistry receipt semantics
remain authoritative. Split coherent routing/service implementation files to
respect 800-line files and 100-line functions.

Keep every existing NativeShuffleTransportTest assertion. Add actual bounded
queue/owner tests for handles=1 repeated exact registration, complete identity
conflicts, barrier-synchronized concurrent replay, lease-versus-release,
STOP/cancel/deadline versus START, clean linger/empty EOF, service shutdown and
retryable cleanup. Use real production leases and barriers, not sleep, mocks,
private access or artificial collaborator failures. Verify actual rows, EOF and
budget accounting, not merely successful component construction.

Nearest Native suites: FlightShuffleServerPortReuseTest,
FlightShuffleResourceBoundTest, FlightShuffleConsumeBarrierTest,
FlightShuffleDemuxerLifetimeTest, FlightShuffleDemuxerPartitionBoundTest,
FlightServerRegistryConfigTest, HandleRegistryTest, NativeResourceDrainControlTest
and the new registration/control tests. After matching packaging run the original
NativeShuffleTransportTest, NativeExchangeTransportTest, WorkerResultPortIntegrationTest
and Worker full regression. Require matching Release full CTest and ASan ownership
checks. Public SQL checks include NativeShuffleE2ETest,
PhysicalTwoWorkerShuffleE2ETest.joinAndGlobalTopKUseBothPhysicalWorkers and
ActivationSmokeE2ETest. Keep distinct-process/endpoint oracles; JNI and direct
component tests are not distributed E2E or production-scale SLO evidence.

New exact selectors:

- `ShuffleProducerRegistrationTest.*`: real bounded ownership, identity conflicts,
  concurrent exact replay, service seal; the conflict test uses actual signed
  FlightClient DoGet to verify nonempty payload and empty-partition EOF.
- `TaskShuffleControlTest.*`: actual admitted START versus receipt/release,
  cancel/release unique watchdog join, committed-producer winner and real deadline
  watchdog versus release; no injected clock, lease or failure collaborator.
- `FlightServerRegistryConfigTest.ActualServiceLeaseBlocksShutdownWithoutHoldingRegistryLock`:
  actual service lifetime/port/registration, shutdown seal and explicit next runtime.

At SOURCE_READY only `git diff --check` had passed. Root subsequently verified
the frozen source closure on 2026-10-07 with a zero-warning Release build,
92 nearest tests (23.52 seconds), unfiltered Release CTest 4052/4052 (568.36
seconds), a matching ASan build, and the same nearest ASan 92/92 (47.77 seconds).
Logs: `/tmp/pangrova-production-20261007-native-shuffle-owner-{build,nearest}-r4.log`,
`/tmp/pangrova-production-20261007-native-release-ctest-r4.log`, and
`/tmp/pangrova-production-20261007-native-asan-shuffle-owner-{build,nearest}-r4.log`.
Release library SHA256 is
`921cca2f4230514ef4e66508f4943148f9bc9bf26267569a49ebbd42966e6a97`.
Matching classpath packaging has completed; the Worker compile/install refresh
explicitly skipped behavior tests. The same frozen cohort subsequently passed
unfiltered ASan CTest 4052/4052 (1640.86 seconds, address instrumentation present,
no leak suppression) and the six original Worker/JNI suites 22/22 (15.183
seconds). Logs: `/tmp/pangrova-production-20261007-native-asan-full-r4.log` and
`/tmp/pangrova-production-20261007-worker-shuffle-jni-nearest-r5.log`.
Public AI/Lance/Smoke r8 failed (18 tests: six PASS, six FAIL, six ERROR), exposing
missing task trace, nested CREATE schema IDs and actual failed-result-reader
drain/shutdown defects. These are separate open closures, not accepted by the
shuffle tests. New source edits do not inherit this artifact's PASS.
Independent-process distributed and production-scale acceptance are not claimed.

The previous occupied-port cohort's Release 4043 PASS, ASan 4 PASS and packaged
JNI port 3 PASS are separate evidence, not acceptance of this new shuffle cohort.
