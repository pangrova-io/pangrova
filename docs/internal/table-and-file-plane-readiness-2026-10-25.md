# Table control and multimodal file planes — implementation and production qualification ledger — 2026-10-25

Status: **production qualification is incomplete**. This consolidated ledger
covers the full P0–P3 Table Control Plane plan and the M0–M5 multimodal file-plane
work against the Databricks/Snowflake alignment design as of 2026-10-25. It records
implementation, repairs, validation evidence and remaining production gates; it
is not production acceptance or a claim that either plan is complete.

Both workstreams were previously paused at the user's requested account-quota boundary
(10% remaining), after committing repairs and completing in-flight focused
validation. All file-plane changes in this checkpoint were committed and pushed
on `codex/ai-native-semantic-closure`.

Evidence below belongs to the source/runtime snapshot identified in each section.
Component tests and results from different snapshots must not be combined into a
passing final-revision gate. The final combined revision needs fresh qualification;
changes in other engine areas need their own current-source evidence.

## ASan dependency preparation — continuation on 2026-10-27

The isolated matching SDK build reached Native configuration and correctly
rejected a mixed RE2/Abseil package with a real hash-container assertion. A
complete matching dependency cohort passes the independent address-check probe;
the SDK is being rebuilt against that cohort. These preparation attempts do not
constitute a passing Native ASan gate.

The RE2 runtime probe also forced leak checking on macOS despite the documented
address-only gate. It now selects address checks on Darwin and retains Linux
leak checks, with halt-on-error on both. Three actual CMake child-process tests
verify the selected environments and that a failed child still rejects the
configuration. They substitute dependency discovery/compilation and do not
claim ABI qualification. The real macOS leak-enabled diagnostic reported leaks
and remains failed evidence; no suppression was added and Linux ASan/LSan remains
an independent mandatory production gate.

## Paimon E2E admission contracts — continuation on 2026-10-27

The frozen shared collection also reached fixtures predating the controlled
maintenance boundary. Bare table arguments were rejected before connector
execution; tag/branch/rollback CALLs additionally lack an authorized maintenance
policy. The fixture repair follows the documented contract rather than reopening
that bypass: tags and branches use authorized DDL, supported maintenance uses
`schema.table`, and unmodeled procedures prove rejection with unchanged durable
snapshots/data. Snapshot expiration first proves that retaining one violates the
default policy, then actually expires twelve snapshots to ten and retains all
twelve current rows. Unsupported `rewrite_file_index` remains unsupported.

All four affected Paimon test classes passed 42 cases with zero failures/errors/
skips in a separate JVM against the frozen `d465fac1` runtime. This qualifies
the fixture/admission and metadata scan paths, not the newer S3 receipt or NULL
predicate fixes. Affected E2E/static verification exited successfully with zero
reported SpotBugs findings; its missing-analysis-symbol diagnostics are retained.
TTL tag metadata is produced by an independent real Paimon SDK
writer and scanned through Native SQL after proving the SQL CALL was denied;
it is not evidence of an authorized TTL SQL mutation API. Rollback connector
effects remain separately tested at the connector boundary. None of these
checks closes an unimplemented design feature or replaces combined qualification.

## Synthetic-column NULL predicates — continuation on 2026-10-27

The frozen `d465fac1` shared E2E collection exposed a real Native read failure:
`WHERE future_field IS NULL` on an Iceberg UNKNOWN column pushed a filter for
that synthetic column into a physical Parquet reader whose schema contains only
`id`. Both NULL predicate handlers now require membership in the physical reader
schema, matching the existing comparison handlers. The authoritative Native
Filter continues to evaluate the predicate after schema evolution supplies NULL.

A new regression failed before repair and all 17 related Native predicate tests
passed after repair. The full matching Release Native gate then passed 4337/4337
with a real owned PostgreSQL/Lance authority in 822.96 seconds. Its immutable
fixture snapshot and all 397 frozen consumer JAR hashes remained unchanged.
Physical-column NULL filters remain enabled. The SQL regression retains its
original three-row assertion and adds IS NOT NULL, AND and OR cases. Updated
JNI/SQL and ASan qualification remain pending; the ongoing frozen collection
uses the earlier JNI and does not qualify this fix.

## S3 committed-write receipts — continuation on 2026-10-27

Two real MinIO regressions failed before repair: PUT and multipart completion
discarded their provider responses and returned the latest object's HEAD. A
second writer between acknowledgement and HEAD caused the first operation's
receipt to report the second writer's version and size. The original failures
are retained. Both accessors now pin metadata to the acknowledged version ID,
with the returned ETag as a condition. A mutable `null` version continues to use
ETag evidence, consistent with [S3 suspended-version semantics](https://docs.aws.amazon.com/AmazonS3/latest/userguide/AddingObjectstoVersionSuspendedBuckets.html).

The fixed diagnostic passed all 13 real MinIO cases and seven actual-SDK/HTTP
receipt cases, zero failures/errors/skips. Missing identity, mismatched evidence,
missing metadata and post-acknowledgement HEAD failure cannot produce a successful
receipt or trigger an accessor-level replay. Versioned concurrent writes return
their own acknowledged generation; both unversioned PUT and multipart races fail
receipt verification after commitment. HTTP fixture corrections retain the
SDK's real checksum validation; a fake ETag that contradicted its four-byte body
was rejected and its failed diagnostic is retained. The first complete default
Worker/static run passed 1441 cases and zero SpotBugs findings before the two
unversioned MinIO cases were added. Its full collection is retained. The expanded
complete Worker/static run passed 1443 tests with zero failures/errors/skips and
zero SpotBugs findings. Final combined-revision qualification remains pending.

The ongoing frozen `d465fac1` E2E collection does not validate this change. No
consumer JAR was installed/overwritten. Durable multipart state, idempotence,
UNKNOWN reconciliation and provider production qualification remain open.

## Default E2E R4 Native crash — continuation on 2026-10-27

The frozen `e74d80e9` fixture / `84411836` production-runtime collection ended
with Maven exit 1 and fork exit 134. Its 1010 completed XML cases report zero
failures/errors/skips, including Q5 and all 12 independent-process cases, but the
JVM crashed in `AiSqlReadWriteE2ETest$VectorColumnOperations`; this is a failed gate.
Original XML, dumpstream, HotSpot report and macOS crash report are retained.
All 397 consumer JAR hashes remained unchanged throughout this invocation.

The crash stack recursively enters the SDK's complex-column schema export
from shuffle serialization, dereferencing a null address while reading an
`ARRAY<REAL>` column. A focused Native regression failed before repair: exact
type/name matching returned the original row without materializing its lazy
array child. The schema bridge then interpreted LazyVector storage as an actual
ArrayVector. Canonical binding now recursively materializes children even when
names match, retaining unchanged wrappers and sharing payload/null/index buffers.

All 15 focused Native binding/type tests passed, including batch and iterative
array serialization round trips. The complete standard fixture-owned Native
gate passed 4336/4336; all 171 fixture consumer JAR hashes stayed unchanged.
The actual vector SQL read/write selection passed 12 cases with zero failures/
errors/skips, using matching Worker and classifier JNI resources `b3bcee6b…`.
The pre-repair log remains failed evidence. Updated JNI consumers then passed
all 1432 default Worker cases/static checks (zero SpotBugs findings) and the
explicit 23-case bounded integration selection, all zero failures/errors/skips.
ASan and a fresh combined E2E/isolated-owner gate remain pending.

Preparing a matching isolated ASan SDK also exposed the pinned SDK's unconditional
Linux `-latomic` flag on macOS/Homebrew Clang. A separate bootstrap portability
patch omits it on Darwin and appends it without discarding existing linker flags
elsewhere. Four actual CMake selection cases passed. The canonical function/
execution SDK patch remains `6799535a…`; this platform fix changes build flags,
not function-generation semantics. Dependency/tool prerequisites and the private
ASan build are retained separately from production qualification.

## Opened-body absolute expiry — continuation on 2026-10-27

Worker now owns both admitted provider streams and a cancellable alarm for each
stream's original signed expiry. The alarm closes an opened body even when no
read returns, using the existing S3 partial-response abort path. Explicit stream
closure cancels its alarm; endpoint shutdown retires alarms and active bodies
before provider clients. Concurrent close callers wait for the same cleanup;
cleanup failures remain observable, and fatal errors are not hidden by earlier
ordinary failures. Admission rejects expiry/shutdown that races with scheduling.

The focused diagnostic selection passed 42 tests with zero failures/errors/skips.
Its actual SDK/socket case proves the reader entered a blocked socket read before
the real timer aborted it at the original expiry, with one GET and no replay.
This is bounded transport fault injection, not external-provider qualification.
The first diagnostic HTTP fixture omitted Last-Modified and correctly failed the
existing metadata-completeness check; that failed log is retained separately.
The complete default Worker module/static verification then passed 1432 tests,
zero failures/errors/skips and zero SpotBugs findings, including all nine real
MinIO cases. The ten deadline/concurrent-cleanup cases also passed 20 deliberate
repetitions (200 test executions). The focused Maven/static selection passed
before the final two cleanup cases were added; the complete module includes them.
The real SDK/socket expiry case additionally passed five deliberate repetitions.
An explicit bounded integration selection subsequently passed 23 cases, zero
failures/errors/skips, covering startup/authentication/cancellation/Native creation
and shutdown. An earlier broad-profile invocation accidentally selected the 5 GiB
capacity case and was stopped (exit 143) to respect the user's dedicated-environment
boundary. Its compressed original log and 419 completed-case reports are retained
as interrupted evidence, not a passing gate. The generated partial upload was
verified against the fixture pattern and closed-file ownership before removal.

These edits are separate from the frozen `e74d80e9` default E2E collection;
that collection does not validate this new implementation. Native/protocol inputs
are unchanged. Pre-stream HEAD/open deadlines, idle timeout, active RBAC revocation
and the remaining P0–P3/M0–M5 matrix are still incomplete.

## Q5 deterministic oracle ordering — continuation on 2026-10-27

The third default E2E collection at fixture `7d43e059` / production runtime
`84411836` ended with 1010 tests, one assertion failure, zero errors and zero
skips. The complete original XML collection is retained. The three real-process
selections passed (local processes 1, speculation 10, AI processes 1); this does
not qualify independent Coordinator failover or the production deployment matrix.

The one failure was Q5's ordered comparison. A separate complete-row diagnostic
proved that Native and PostgreSQL returned the same 14-row multiset within the
existing numeric tolerance. JAPAN and JORDAN have exactly equal revenue
`-38403.376`, while the original SQL ordered only by revenue. Their relative
order was therefore unspecified. The bounded E2E SQL now adds `n_name ASC` as a
secondary key, and Q5 additionally verifies the fixture's exact revenue tie and
country order. Complete row-by-row comparison remains enabled; the temporary
diagnostic was removed and the benchmark workload was not changed.

The Q5/strict-comparator selection and E2E static verification passed all eight
cases with zero failures/errors/skips. A fresh unified default E2E collection and
the four isolated Native owners remain pending for this final fixture correction.
Production/runtime JAR inputs remain those of `84411836`; no install or artifact
overwrite occurred during these consumers.

## Bounded runtime and pooled E2E metadata — continuation on 2026-10-27

Production inputs at `84411836` passed the affected complete default owners/static
checks: Worker 1422, Metadata 566 and Coordinator 2939, all zero failures/errors/
skips and zero SpotBugs findings. The first separate-checkout Coordinator attempt
omitted the required Lance tool path; its two prerequisite failures and interrupted
invocation remain retained. The corrected complete Coordinator run uses the
matching built tool explicitly. No test or timeout was weakened.

After installing the three changed owners, the frozen 354-JAR runtime manifest is
`a5f69d27555d8a13c94818bc4eb770ac210fefa706d09c4665c9945b47fbdee6`.
Its Worker and classifier both contain JNI `a59914ea…`; direct dynamic dependency
hashes match the prior receipt. Native/protocol/POM inputs are unchanged from the
4334-test Native gate at `19b5ffe1`. The consumer JAR hashes remained unchanged
through the subsequent checks; no Maven install overlapped these consumers.

The expanded 63-case endpoint/Native/TPC-H invocation recorded six 30-second
planning/submit timeouts (Q2/Q5/Q7/Q8/Q9/Q21); it is failed evidence. All 14 actual
table lifecycle cases passed, including the shadow-DROP physical UUID/one-row
preservation case. A thread snapshot showed SCRAM password derivation for a new
PostgreSQL connection inside repeated ownership checks: the shared E2E fixture
used an unpooled DriverManager datasource, unlike production.

The shared fixture now uses the existing bounded production connection-pool
factory. Its owner closes the pool after stopping artifact consumers and metadata,
and closes partially initialized resources on startup failure. A real-PostgreSQL
case verifies backend reuse over 32 logical connections. With this correction,
all 22 TPC-H queries and four deployment/bootstrap cases passed with static
verification: 26 tests, zero failures/errors/skips. Ownership checks and the
30-second submission limit remain intact. These are bounded correctness results,
not production latency measurements. A fresh default E2E collection, independent
processes and all four isolated Native owners remain required for the final
test-fixture revision. The P0–P3/M0–M5 implementation matrix remains incomplete.

## Read admission after provider I/O — continuation on 2026-10-27

Worker rechecks the captured descriptor after endpoint HEAD, before opening the
provider body and after that open returns. An expired HEAD cannot disclose range
metadata or start the subsequent read; an expired open is closed before an HTTP
success response is constructed. Cleanup failure remains suppressed on the
original access denial. HEAD-only responses likewise recheck before returning
their metadata. The original delivery deadline remains on the admitted stream.

Five actual-SDK/HTTP fault-injection and cleanup cases passed, together with the
existing deadline/resource/HTTP selection: 52 tests, zero failures/errors/skips.
These ran in a separate diagnostic output directory so the ongoing reactor's
compiled classes remained frozen. Worker and E2E Checkstyle passed. A separate
checkout then passed all 1422 default Worker tests and static verification with
zero failures/errors/skips and zero SpotBugs findings. Its initial invocation
omitted `native-runtime` and failed because the JNI resource was absent; that
failed log/XML is retained separately. The corrected invocation loads the
matching classifier instead of disabling Native tests. Refreshed combined
endpoint/Native checks remain pending. This is
boundary revalidation, not a provider-call timeout, blocked-I/O/idle watchdog,
active-RBAC revocation or completed transfer qualification.

## Default-reactor policy fixture repair — continuation on 2026-10-27

The full default reactor at `a5ea42e0` reached E2E and recorded six failures in
`DmlCopyOnWritePolicyE2ETest`: its fixture persisted name-only policies, which the
transaction-bound production authorization correctly rejected as unproven. The
run was intentionally stopped (exit 130) after retaining the original log and
failure XML. It is failed/incomplete evidence, not a passing full-reactor gate.

COW and the related subquery fixture now enable durable controlled registration
before creating their actual tables and publish through Metadata's stable row
policy/mask stores. They restore the previous control owner during teardown. COW
also asserts that persisted policies and masks carry the created table ID. All
physical-survivor, masked-predicate, NULL, grant-revocation and correlation assertions
remain intact. The six COW and six subquery Native SQL cases passed with zero
failures/errors/skips, followed by E2E static verification. The fail-closed rejection
of unbound policies remains unchanged.

The second full default reactor used the compiled `19b5ffe1` snapshot and ended
with 13246 tests, zero assertion failures, five errors and zero skips. Its E2E
portion reported 991 tests and the five errors; failed class initialization does
not mean the undiscovered nested query cases passed. The COPY fixture ownership error came from the private
ticket/restart method had `native-runtime-owner` but lacked the `native-required`
tag used by the other three owners, so the daily selection entered it in the shared
Native JVM. The ownership guard rejected before modifying runtime state. The
missing tag is restored; the case remains mandatory in its isolated JVM selection.
The failed report is retained and this invocation is not a passing reactor gate.
Three independent-process fixtures rejected the inherited `LC_ALL=C` before
startup; their non-ASCII CLI credential tests require a real UTF-8 native locale.
The remaining error occurred when TPC-H setup replaced an unchanged Catalog
deployment and DROP completion rejected its old control record. Corrected
independent-process runs and all four isolated owners remain pending.

Shared connector registration now retains a matching provider/properties/tenant
definition and still ensures the requested schema. A changed warehouse cannot be
hidden by the former catalog-name-only shortcut. Three actual deployment/bootstrap
cases and the three non-owner COPY cases passed, with E2E static verification:
six tests, zero failures/errors/skips. These validate the fixture change against
the frozen installed runtime; they are not combined-source production acceptance.

The independent Native gate at `19b5ffe1` passed all 4334 cases using frozen
installed fixture JARs and the matching isolated SDK/JNI cohort below. The staged
runtime manifest for that Java snapshot is
`7ba3039c7e142507945e074c90b00bc6c9d3bc2b64cf2eb4a50f3d4c04b9ddae`.
The new read-admission source repair is not part of these running/frozen artifacts.

## Shadow DROP deployment admission — continuation on 2026-10-27

The TPC-H failure also exposed a production ordering gap: shadow-mode DROP could
delete the physical table and only then reject retirement of a control record
from a replaced Catalog deployment. Both SQL DDL and the trusted Metadata API
now validate the captured record's deployment before physical DROP. Completion
keeps its existing revision/deployment checks. The Metadata path also keeps the
same control-service owner throughout the operation.

The affected Metadata/DDL ownership selection and static verification passed all
49 cases. Two regressions retain the original control record and physical table
after a same-UUID Catalog replacement in shadow mode. Complete owner suites and
the new real-SDK/HTTP SQL preservation case are pending. This does not provide an
atomic Catalog lease across I/O or automatic reconciliation after a concurrent
deployment change.

## Iceberg rename identity fence — continuation on 2026-10-27

Rename completion now rejects a changed Iceberg table UUID, preserving the pending
record and both name reservations for reconciliation instead of transferring old
governance to a replacement. The positive test retains the UUID and verifies that
an unavailable post-rename version clears the previous known version. The generic
cached-owner test uses Paimon's name-scoped identity model instead of incorrectly
modeling a changing durable Iceberg UUID.

Focused control-service tests and Metadata static verification passed. Five real
JDBC SDK cases/static verification passed, including cross-namespace rename keeping
the same table UUID and exact metadata pointer on the locked candidate4 SDK. The
[Iceberg specification](https://iceberg.apache.org/spec/#table-metadata-fields)
requires rejection of UUID mismatches. This does not complete Paimon/other-format
rename proof, automatic crash reconciliation or the final refreshed reactor gate.

## S3 checksum evidence — continuation on 2026-10-27

S3 HEAD and listing no longer infer content MD5 from a 32-hex ETag. Encryption,
multipart and compatible-provider semantics do not permit that inference; the
current generic accessor reports unknown MD5 while preserving opaque ETag and
provider-version conditions. This follows the [AWS object contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html).
Three SDK metadata policy cases cover encryption declarations/customer encryption
and listing. They are unit evidence, not encrypted-provider qualification. Nine
bounded real-MinIO cases passed, retaining full-body/range checks and checking that
the fixture's MD5-looking ETag is preserved without inventing MD5. Complete default
Worker tests and static verification passed. A refreshed combined Java/runtime
cohort is still required after the remaining reactor repairs below.

## Frozen bounded runtime checkpoint — 2026-10-27, implementation 2367c831

The isolated SDK checkout uses the branch-required patch cohort
`6799535a1bdf87ce9af3f559546dbbe10ef244c210f3a19f52cf6fbf56bbeadb` on
Velox `3da0390118fa4b0addbfbdeb03f438ec1b1237a6`. Its copied third-party package
configs and generated xsimd cache entry were relocated to the owned dependency
snapshot; no compiler warning rule was relaxed and the shared SDK checkout was not
reverted. The earlier diagnostic run using the other SDK patch failed provenance
and remains failed evidence. The corrected isolated Release build passed all
**4334 Native tests**, including provenance checks. The mTLS case then passed
**20 consecutive runs** with stop-on-first-failure semantics; this is bounded
current-cohort evidence, not a demonstrated cause for every historical intermittent
failure or Linux qualification.

Current artifact SHA-256 receipts:

- Release JNI: `a59914ea232a6f724dda2bcf4d565e2c34bcb76a7557b46c8f5642c99f74bbf2`.
- Native test executable: `1a1da6c88cc43782f47f529b9844cebfcc1eed223f7a876577278a89c7bb8f0b`.
- Velox archive: `f68ba80497e29057ece35c96f8e5485932531122e6a622b412b61ced1285690b`.
- Lance FFI archive: `786a171a8eb8ba416a82652c3502fe3c4fd83ea360c672bce96e1a47d367ee98`.
- Frozen runtime JAR manifest: `700b66acdfd3783749a67162fc511c1344b7bd3474cd649487d237bc16d9ca9b`.

The JNI classifier and Worker resource contain the same new library. After
packaging, the complete Java reactor installed successfully; its JARs were frozen
before any E2E consumer started. The same bounded **34-test** endpoint/Native
selection passed with zero failures/errors/skips: Stage HTTP 8, file contract 3,
file DDL 5, table control lifecycle 13, Lance Feature View export 2 and activation
smoke 3. The real AI fixture used immutable image
`sha256:e10dbd499c14b15dfe7bc7bc778d45d2f1554add090d8eba6b322ae111a8ec2a`.
Four isolated runtime owners then passed with the same staged JAR manifest and JNI:
Worker bootstrap 5, external Native Exchange 3, Shuffle admission 1, and the private
COPY-ticket restart case 1, all with zero failures/errors/skips.

The affected default module suites/static checks also passed: Metadata 564,
Coordinator 2938 and Worker 1414 current reports, all zero failures/errors/skips.
Default profiles exclude their configured integration/Native groups; these counts
are not a full production-regression result. The full default Java reactor test
is the next shared-SPI check. The implementation and production matrices below
remain open, including schema-bound governance, real quality execution, immutable
Directory manifests/Native execution, durable multipart recovery and the deferred
large-data/Linux/provider qualification. No scope is closed by combining historical
results with this checkpoint.

## Invalid provider receipt diagnostics — continuation on 2026-10-27

Coordinator validates that a Worker URL receipt contains a nonblank URL and positive
expiry. Null, incomplete and malformed JSON receipts reject with a storage error.
Payload parsing has a separate sanitization boundary: Jackson messages and causes
can retain credential-bearing response content, so neither is attached to the
outward exception. HTTP transport failures remain distinct from payload validation.
Real HTTP response tests cover null, missing, blank, invalid-expiry and malformed
receipts and assert that a signature sentinel is absent from the full exception
trace. Focused client/service tests and Coordinator static verification passed;
complete default Coordinator regression and the frozen bounded runtime checkpoint above also passed.

## Directory diagnostic owner fencing — continuation on 2026-10-27

Diagnostic directory pages previously selected the current Stage by name, allowing
an old captured FileSpace to read a same-name replacement's index. They now validate
the exact captured Stage ID and definition revision and hold a shared definition
lock for the one page transaction. The index query uses only the admitted ID.
Reference admission uses the same check. Ten real-PostgreSQL cases passed, including
same-name replacement and storage-profile revision changes, followed by Metadata
static verification. Complete default Metadata regression and the frozen bounded
runtime checkpoint above also passed. These diagnostic pages do not provide immutable generations, manifest
publication or Native DIRECTORY execution.

## Provider URL expiry receipts and audit admission — continuation on 2026-10-27

The experimental storage SPI now returns the signed URL together with the actual
signing operation's provider-reported conservative expiry bound. S3 uses the pinned
AWS SDK's public `expiration()` receipt rather than calculating expiry before
credential resolution and signing. Worker and Coordinator both require a still-live
receipt no later than the captured authorizing grant; the response reports that
admitted bound. Credential-bearing result diagnostics redact the URL.

Provider-URL success audit is emitted only after the Worker call and receipt
validation succeed, using the originally captured principal and FileSpace. Failed,
expired and overlong receipts produce no success event; tests drain the audit queue
before asserting absence. SPI tests/static verification, complete default Worker
and Coordinator tests/static verification, and nine bounded real-MinIO cases passed.
Stable Stage links likewise capture the authorized object/principal and emit success
only after URI construction succeeds; valid and failed construction cases passed
with the audit queues fully drained, followed by Coordinator static verification.
The refreshed bounded Native/end-to-end cohort is recorded above. This does not provide
immediate revocation of provider URLs, STS credential lifetime qualification, durable
multipart recovery or production-provider acceptance.

## S3 byte-window proof and partial closure — continuation on 2026-10-27

An opened S3 read now proves the requested byte window using returned content length,
Content-Range start/end/total, and the existing ETag/version admission. A missing,
ignored, mismatched or overflowing window is aborted before delivery. Zero-length
reads avoid an invalid provider GET; negative lengths other than the documented -1
are rejected. Stream counters reject truncated/excess bodies. Fully consumed bodies
use normal SDK closure; partial/failed reads abort the connection without draining
unread provider bytes, including cancellation through the delivery-deadline wrapper.

Eight SDK-stream/response unit cases and nine bounded real-MinIO cases passed with
zero failures/errors/skips, followed by Worker static verification. The new real
case stops after seven bytes of a 32 KiB object and successfully reads another exact
range with the same client. Fault-injected responses are unit evidence, not an
external-provider oracle. This does not implement an idle-read watchdog, live RBAC
revocation, autonomous multipart cleanup or the large-object production matrix.

## Worker file endpoint ownership — continuation on 2026-10-27

The Worker HTTP server now retains and closes its FileSpace endpoint, including
failed startup; constructor failure releases the newly allocated endpoint too.
Endpoint destruction releases the accessor factory's clients/presigners. The factory
fences resolution against closure, waits for the one cleanup result and preserves
failure across repeated close calls. It owns a separate default credential chain;
caller-supplied credential providers remain borrowed and are not closed. The AWS SDK's
shared `create()` singleton is not used for an owned credential chain.

Focused lifecycle, actual HTTP startup and file endpoint tests passed, followed by
all default Worker tests and the eight bounded real-MinIO transfer cases (5 MiB parts,
not the deferred large-object gate). Static verification also passed after correcting
line length, Javadoc and aggregation braces, and narrowing the server's retained
reference to the endpoint's destruction capability. Repeated close calls retain the
same failure instead of allowing lifecycle-hook logging to hide it. The combined
refreshed-runtime endpoint/Native checkpoint is recorded above. This is resource ownership, not durable upload reclamation,
active stream revocation or a completed transfer deadline/watchdog implementation.

## Typed Metadata creation — continuation on 2026-10-27

The engine Metadata creation path previously forwarded names without typed columns,
so real connector schema builders could receive an empty declaration. It now forwards
scalar types and explicit DECIMAL precision/scale and VARCHAR length. Parameters are
validated before connector creation. Incomplete structured declarations and unknown
types reject instead of inventing children or field IDs; actual format allocators
still own IDs. This does not complete Metadata's whole DDL model or the REST-catalog
compatibility surface.

The focused declaration/control tests and complete Metadata tests/static verification
passed, followed by module installation and a frozen combined 34-test endpoint/Native
selection with zero failures/errors/skips. The added case creates a managed table
through the trusted engine Metadata API, then uses authenticated HTTP SQL for an
actual Native INSERT/SELECT: DECIMAL(12,2) retains `12.34`, and a directly assigned
uppercase PII tag masks the declared VARCHAR column. The fixture's metadata-only
actor is not a claim that the REST catalog authenticates or implements that path.

## SDK cohort requalification — continuation on 2026-10-27

The complete Release CTest invocation returned 4,333 passing tests and one failing
gate (`canonical_function_provenance`); the mTLS case passed in that invocation,
which does not resolve its historical intermittence. The shared SDK source/archive
had moved to patch `9b14ba80…`, whereas this branch requires `6799535a…`. The gate
correctly rejects that cohort. The rebuilt diagnostic test executable consumed that
shared archive, so its results do not qualify the existing JNI package or close the
final-source Native gate. The packaged JNI remained `4e6a3e1c…`; the separately frozen
32/33-test HTTP/Native checkpoints above retain their own artifact evidence.

No production Native code or provenance requirement was changed. Temporary listener
experiments rejected a second same-port server for both explicit-false and default
configuration on this host; the experiments were removed after not reproducing the
failure. A separate SDK checkout at the pinned Git commit with this branch's exact
reviewed patch is being built under an isolated cohort directory. Shared SDK state
is not reverted, and acceptance requires its own matching archive/source receipt.

## Canonical column tag admission — continuation on 2026-10-27

The default Metadata SET/UNSET/list API resolves actual canonical columns under the
caller's connector session. DDL no longer has a separate case-insensitive type check
that can persist a spelling different from Native's field name. Exact names remain
distinct; a folded name must have one match. PII type validation is enforced for direct
API callers too. Fresh physical ownership is checked around declaration resolution,
missing/changing schema fingerprints reject admission, and storage receives the captured
control owner for persistence/removal/listing instead of resolving another name owner.

Focused Metadata tests passed, including canonical names and ambiguous case, missing
or wrong types, changed schema evidence, external replacement during resolution and
control UUID replacement. Captured storage-owner rejection and complete Metadata tests/static verification
also passed. The refreshed Java reactor installation and complete Coordinator tests/static verify
passed. The frozen runtime for `3baebc71` passed the combined 33-test endpoint/Native
selection with zero failures/errors/skips. Actual HTTP SQL now supplies uppercase
SET/UNSET column names while Native verifies all four PII masks, NULL/short-string
behavior, rename and name reuse. A separate SDK external replacement followed directly
by tag DDL is rejected and quarantined without publishing any tag binding. This is admission repair;
durable field binding and atomic external-schema continuity remain open.

## Typed selected-schema evidence — continuation on 2026-10-27

`ConnectorReadBinding` now carries optional typed schema identity, checked against
its exact immutable declaration. Evidence identifies the physical owner, allocator
namespace, selected schema fingerprint and actual top-level field IDs. Stability is
explicit: `SELECTED_SOURCE` remains tied to this binding's source identity, whereas
`NON_REUSABLE_NAMESPACE` asserts the format's non-reassignment rule in that owner/domain.
Missing evidence, ordinals and virtual metadata fields are not promoted into proof.

Iceberg builds the selected declaration and schema identity from one resolved schema;
the ordinary case captures immutable table metadata for schema/properties together.
Unavailable historical declaration evidence keeps schema identity unknown. Paimon
retains the fingerprint of the actual current or historical schema and identifies
the SDK branch namespace. Its pinned 1.4.1 SDK can roll schema files back and reuse
field IDs, so it publishes only `SELECTED_SOURCE`. Lance remains unknown: its
connector does not expose format field IDs and the pinned 0.20.0 allocator does not
establish the newer non-reuse capability.

All read-binding constructors were migrated explicitly, including defaults,
metadata/derived sources and the retained Paimon limit wrapper. The SPI complete
test/static installation and real SDK focused selections passed. An Iceberg case
keeps an ID through rename and allocates another on replacement. A Paimon case
actually rolls an empty table's schema back and demonstrates reuse without claiming
non-reusability; current and historical fingerprints are checked separately.

The full Java reactor installation, complete Planner tests, dependent Coordinator
read/derived-read selection and complete Iceberg/Paimon/Lance tests with static
analysis passed. Static verification first exposed an existing Lance constructor
Javadoc omission and two null-path findings; local source aliases retain the same
null rejection and make that control flow explicit. Two new Iceberg line-length
errors were also corrected before the complete successful rerun. The final reactor
refresh passed. The frozen installed runtime for `8686dca9` passed the combined
32-test endpoint/Native selection with zero failures/errors/skips (stage URLs,
file-space contract/DDL, table ownership, Lance Feature View export and activation). This is read-side
evidence, not completed field-bound governance. Policy persistence, authenticated
publication, query-bound policy consumers and deferred receipts still need migration;
the schema identity alone cannot authorize cross-version policy reuse. The design
and version boundaries are recorded in architecture review §6.4.

## Query-local read ownership — continuation on 2026-10-27

Each cached read now retains the control owner captured with its immutable source.
Every planning lookup revalidates that owner and the name's current UUID. An
OBSERVED binding cannot inherit a later ADOPT; UNLINK/re-ADOPT of the same physical
table cannot attach a new control identity to an old read. Pending rename and a
changed captured physical identity reject reuse without quarantining a healthy
new owner on stale evidence. Ordinary data-version observations still permit the
same selected immutable snapshot.

The Metadata ownership unit selection and Coordinator real-SDK read/derived-read,
deferred index, index-read and Feature View selection passed, along with the
PostgreSQL canonical tag/row-policy, artifact-authority and frozen COPY admission
selection. This includes the preceding deferred-applicability guard. The combined
Java reactor installation passed. The combined endpoint/Native selection passed
all 32 tests with zero failures/errors/skips for `92dc3e28`, including the preceding
deferred-applicability guard. Complete Metadata and Coordinator tests/static verify
also passed for that source. Security complete tests/static had already passed with
the same applicability implementation. This does not
close atomic query-bound policy identity, schema/field identity or rename recovery.

## Deferred policy applicability — continuation on 2026-10-27

Policy rendering now rejects an enabled role-scoped restriction whose applicability
cannot be proven for a persisted subject without authentication groups. Previously,
the renderer could omit that row policy or mask before checking missing expression
context. A constant restriction could therefore disappear from raw background
authorization. Foreground and transaction-bound readers share the guard; matching
durable roles, existing applicable exemptions, and disabled policies are covered
separately. Ordinary authenticated group-role resolution is unchanged.

The row/mask unit selection and two real PostgreSQL cases passed. They reproduce
actual group-role policy application in the authenticated foreground, reject both
raw and transformed replay with missing applicability, then grant a durable matching
role and accept the captured transformed scope while continuing to reject raw reads.
Complete Security tests/static analysis, focused dependent Metadata/Coordinator
selections, complete dependent module/static verification and the combined 32-test
endpoint/Native selection passed at `92dc3e28`. This is
conservative rejection, not a reconstructed authentication context or qualification
of every deferred workflow.

## Missing quality execution fails explicitly — continuation on 2026-10-27

The definition-only executor has been removed. Both Coordinator bootstrap paths
now select `UnconfiguredQualityRuleExecutor` until an authorized data-plane query
runner is supplied. An attempted evaluation reports that the rule was not
evaluated; a nonblank expression can never produce a passing result by itself.
The existing handler and pre-commit gate persist failure with the captured table
UUID. An enabled ERROR rule blocks publication before durable write intent;
WARNING/INFO retain their documented nonblocking severity and failed audit.

The focused governance handler, gate and SQL-executor selection passed. Both
production-bootstrap/PostgreSQL checks and the actual Native INSERT regression
passed: the rejected INSERT leaves the original row visible and appends one failed
audit result for the captured owner. The complete governance clean verify/static
analysis, affected PostgreSQL quality/lineage selection and Java reactor installation
passed. The runtime JAR contains the explicit unconfigured executor and no deleted
definition-only executor. The combined SQL/Native selection passed all 32 tests
with zero failures/errors/skips for `6397ea1c`: Stage file HTTP (8), file contract
component (3), SQL Stage operations (5), table lifecycle/quality Native SQL (11),
Lance secure export (2), and activation smoke (3). The read-admission revision-value
repair is included. The complete Coordinator module and affected static checks
also passed for that checkpoint. These results do not include the subsequent
deferred-applicability and cached-ownership changes above.

The first governance owner run failed because the amd64 reference Marquez server
did not become ready under QEMU within its existing timeout. The prepared arm64
fixture retains the unmodified pinned upstream application JAR, SHA-256
`6b5a7ace21b6e3b0d98458abacf93a6dfbe55cd1cb784f1f22a8133d6a966c6a`;
the failed selection and then clean complete governance/static verification passed
with that image. New bootstrap tests initially inserted a catalog definition without
loading its local handle; the real deployment fence rejected it. The fixture now
registers the catalog normally and both bootstrap checks pass. Neither failure was
hidden by weakening admission or readiness checks.

This removes false success; it does not implement actual evaluation,
candidate/post-image checks, source/policy receipts or crash recovery for validated
writes. Those remain required before data-quality acceptance.

## Controlled SQL read admission — continuation on 2026-10-27

Controlled SELECT now checks fresh connector format identity before and after
binding its source. External DROP/re-create under the same name is rejected and
the contradicted owner is quarantined before any write or destructive DDL has to
observe it. The captured control UUID must still own the name after binding.
Namespace components are compared individually, including quoted identifiers that
contain dots. Missing identity evidence rejects admission without quarantining a
table on absence alone.

Read admission permits an ordinary data-version observation to advance while an
immutable snapshot is selected. Retirement, namespace changes and replaced catalog
deployments still reject the old admission. Catalog revision comparison uses value
equality, with a regression above the boxed-Long cache boundary. Publication/write
fences retain their stricter metadata revision check.

The focused Metadata tests (50), Coordinator real-SDK read/derived-read selection,
Java reactor installation and affected static analysis passed. The complete
Metadata owner then passed for this read-admission source. The preceding
`a7bf2df2` complete Governance/Metadata/Coordinator owner run also passed. An
endpoint/Native selection passed 31 tests before the final boxed-revision repair;
the combined current-source run is still required. These results do not close
query-lifetime policy binding, stable column-schema identity,
actual quality evaluation, Native DIRECTORY, durable multipart or recovery gates.

## Quality ownership and HTTP binding — continuation on 2026-10-26

Quality definitions and audit results now carry the captured table UUID. Production
bootstrap uses the stable ownership adapter for current rule reads, table lookups
and scheduled reloads. Rename projects current diagnostic names; old-name reuse,
pending rename and replaced catalog deployments cannot execute another identity's
rule. V101 preserves unproven historical rules/results as unbound. An enabled
unbound rule cannot run through the adapter; no current name is adopted as proof.

The permanent `quality_rule_identity` owner remains after rule deletion. A numeric
rule ID cannot be reassigned to another table UUID. Definitions and results refer
to that owner, so a late result can still be appended after deletion and cannot be
attributed to a replacement. The PostgreSQL lifecycle/upgrade selection passed,
including identity-safe upsert, captured-ID deletion/name reuse, late audit results,
legacy preservation and the existing frozen COPY upgrade. Governance producer
checks preserve the captured UUID on both normal and failed evaluations.

This is ownership, not actual data-quality acceptance. Bootstrap still chooses the
definition-only default executor. An authorized Native quality query runner,
expected-source identity at query admission, policy-content mutation fencing and
validation of the actual candidate write/post-image remain open. String validity
or a scan of already committed data cannot qualify the incoming write.

The preceding `bd7e2fa5` owner run passed Metadata but Coordinator reported one HTTP
protocol error: a profiler request received an SSH banner. The HTTP configuration
adapter discarded `coordinator.host`. A local socket reproducer confirmed that a
later address-specific listener can overlap a wildcard listener on this macOS host;
the failed port was owned by QEMU when inspected. Commit `22078665` carries the
configured bind host through bootstrap to Jetty. Authenticated local startup probes
now bind explicitly to loopback, and a real socket regression proves another
loopback address is not served. Config translation and property/startup tests pass.
This does not establish that the earlier Native mTLS failure has the same cause.

The combined Java reactor installation and Governance/Metadata/Coordinator static
analysis passed. The final combined selection passed all 30 tests, zero
failures/errors/skips: Stage file HTTP (8), file contract component (3), actual SQL
Stage operations (5), table lifecycle Native SQL (9), Lance secure export (2), and
activation smoke (3). These are the existing execution paths, not Native DIRECTORY
or actual data-quality evaluation. Release JNI remains unchanged at SHA-256
`4e6a3e1cacc646d3666c305092ef629d0ab850181f0b1c7313c520fe9196af38`.
Complete module and final production regression still require the resulting source.

## Current file authority — continuation on 2026-10-26

File issuance now resolves the authenticated principal against current PostgreSQL
tenant membership, requires ACTIVE status, enforces the tenant catalog/schema
allowlists and rejects a conflicting tenant claim. Claimless authenticated subjects
work when actually mapped; a claim alone cannot invent membership. The authority
read gate covers the complete namespace/profile/RBAC resolution and signature
issuance for ordinary file grants, presigned-URL grants and stable Stage links.
Role, tenant, namespace and profile mutations cannot overtake a read already inside
that boundary; a request queued behind a committed revocation sees the revoked
state. Lock/storage failures reject issuance using the existing bounded timeouts.
Stage URLs delegate to the issuer's gate rather than holding a duplicate connection.
Production HTTP and SQL Stage bootstrap both supply the durable authority.

Five real PostgreSQL subject/concurrency tests passed. The focused Coordinator
file/Stage regressions, Java reactor installation and Metadata/Coordinator static
analysis passed. The combined endpoint/SQL/Native selection passed 28 tests with
zero failures/errors/skips: `StageFileUrlHttpIntegrationTest` (8),
`FileSpaceControlPlaneContractE2ETest` (3), `FileSpaceDdlE2ETest` (5),
`TableControlLifecycleE2ETest` (9), and `ActivationSmokeE2ETest` (3).
It verifies a normal file-ticket request queued behind membership removal fails,
claimless HTTP authentication, actual SQL Stage operations and unchanged Native
classification/row filtering. The file contract component now uses real PostgreSQL
namespaces, profiles, tenant membership and RBAC rather than allowing fake policies.

The complete Worker/Coordinator rerun of preceding commit `82657b7f` passed.
Metadata/Coordinator complete reruns for this new authority checkpoint are in progress.
This remains bounded ticket issuance, not revocation of already issued bearer tickets,
active streams, provider URLs or durable multipart sessions. No final reactor or
production-scale acceptance is inferred from this selection; other ledger gates stay open.

## File-session authority binding — continuation on 2026-10-26

File tickets now carry stable namespace ID, authenticated subject/tenant and the
exact storage-profile incarnation/revision returned by one current-definition
lookup. Multipart signatures and originating-Worker provider-ID checks bind these
fields together with the complete physical storage scope and path. A new grant
from another principal, tenant, namespace or profile version cannot resume the
old session even when the physical path is identical. Rename keeps the namespace
identity; DROP/re-CREATE cannot adopt it. Missing authority fields and older
session signatures fail closed. Upgrade requires fresh tickets/new sessions.

The reactor installation, focused Worker/Coordinator regressions and both owners'
static analysis passed. The corrected combined selection passed all 22 tests,
zero failures/errors/skips: `StageFileUrlHttpIntegrationTest` (7),
`FileSpaceControlPlaneContractE2ETest` (3), `TableControlLifecycleE2ETest` (9),
and `ActivationSmokeE2ETest` (3). The HTTP regression uses actual PostgreSQL RBAC,
authentication filters, Coordinator issuance and Worker LOCAL multipart bytes.
The registry correctly forbids overlapping external locations; the test exercises
rename followed by DROP/re-CREATE at that location instead of weakening the guard.
Complete Worker/Coordinator module reruns are in progress at this checkpoint.

An already issued ticket remains a bearer capability until its original deadline;
subject binding does not authenticate its holder or provide immediate revocation.
This is not durable multipart state, autonomous cleanup, UNKNOWN reconciliation,
current tenant-membership qualification or the final production regression.
No heavy-data test or native source change is included.

## Stable row-policy ownership — continuation on 2026-10-26

Production bootstrap now reads and saves authored row policies through the stable
table ID. Canonical rows survive rename without a name migration; diagnostic names
are projected from the current registry. A different table under the released name
cannot inherit or overwrite those policies, and deleting the captured original
policy ID leaves replacement policies intact. Both reserved rename names and a
replaced catalog deployment reject policy reads. Deferred artifact authorization
uses the same UUID lookup on its existing publication transaction.

V100 preserves historical name-only rows as unbound instead of guessing ownership.
An enabled unbound row fails closed until explicitly verified and rebound or
retired. Upgrading an existing populated policy table preserves expressions and
priorities, replaces the old name-only uniqueness constraint, and does not advance
the authorization revision merely for an unchanged policy migration.

The Java reactor installation passed with JDK 17 and Python 3.12. Security,
Metadata and Coordinator module test invocations passed, together with a separate
populated-policy upgrade regression and changed-owner Checkstyle. Security,
Metadata and Coordinator static analysis also passed. The focused
HTTP SQL/Native selection passed 14 tests, zero failures/errors/skips:
`TableControlLifecycleE2ETest` (9), `LanceFeatureViewExportE2ETest` (2), and
`ActivationSmokeE2ETest` (3). The fixture now shares production JDBC table ownership
between foreground SQL and background publication instead of assigning unrelated
in-memory identities. Rebuilding the query pipeline and reopening a store do not
qualify independent-process recovery. JNI sources/artifacts are unchanged.

This closes the tested table-identity path for authored row policies. Quality-rule
identity, column-schema identity, immutable directory manifests, durable multipart,
managed ingestion, active stream revocation and final production qualification
remain open; the deferred heavy environment gates remain deferred.

## Stable classification and rename-reservation checkpoint

The current continuation replaces the duplicate name-keyed `column_tags` store with
canonical `COLUMN_TAG` governance bindings. Foreground masks and deferred raw-read
publication now derive classification directly from stable-ID bindings. Production
bootstrap no longer depends on an event listener updating a second mask authority.
Authored column masks persist a table ID; retirement and delayed notifications target
only the captured original identity. Unknown historical ownership fails migration or
policy reads rather than being adopted by name.

Physical rename persists its source and operation identity and transactionally reserves
both names. Both source and target remain blocked through restart while RENAMING.
CREATE, ADOPT and governance cannot overwrite a pending rename. Connector exceptions
retain reservations because an exception or one source-path observation cannot prove
that an in-flight physical rename will never commit. Automatic crash reconciliation
remains a separate open requirement.

Real HTTP SQL validation found that the security expression parsers used the SQL
standard function table, giving `CHAR_LENGTH` INTEGER while Native requires BIGINT.
They now use the same typed operator contracts as ordinary SQL. The regression covers
all three length aliases in masks and row filters, including substring positions.
The focused final migration/control/policy verification passed: 84 tests, zero
failures/errors/skips. It includes the PostgreSQL upgrade from V64 with an existing
frozen COPY input, rename reservation across store restart, name reuse, canonical
classification without event projection, and deferred raw-read rejection. Empty
backfills now avoid issuing UPDATE statements that would unnecessarily advance the
authorization revision and revoke an unrelated existing COPY root. Reservation
maintenance touches only the affected names, with no whole-map sweep or redundant
reservation rewrites during ordinary version observation.

Security and Governance owner regressions passed, as did Planner and Worker daily
regressions. Metadata's complete post-upgrade rerun at `27d84e07` passed all 830 tests with zero
failures/errors/skips after the upgrade correction. Coordinator's first complete
run had 3,346 tests and one unbound-mask fixture error; the corrected real ADOPT /
identity-bound fixture and ownership/SDK regression passed all 35 selected tests.
Checkstyle passed for the changed Java owners. Metadata, Coordinator, Security and
Governance owner static analysis passed; the entire reactor remains unqualified.
Static analysis is not globally green: unchanged materialization reports nine
representation-exposure findings and unchanged planner materialized-view rewriting
reports two. These remain explicit gates; the reviewed Worker SDK-client classification is described below.
Native sources are unchanged in this checkpoint.

This checkpoint does not implement immutable directory Manifests/Native DIRECTORY,
durable multipart ownership/reconciliation, FILE MANAGED ingestion, active-stream
revocation, or the rest of M0–M5. It also does not qualify authored row-policy/quality
rule identity, independent-process recovery or the production deployment matrix.
The previously deferred heavy data/environment gates stay deferred.

## Paimon metadata freshness and final classification SQL evidence

The first corrected Native SQL run passed masking/rename but failed writing a new
schema under the released original name. A separate Worker's real SDK catalog held
the old cached table schema; SDK `uuid()` could simultaneously read the new table's
creation time. Authority now invalidates the SDK name cache before each fresh
metadata resolution, including writes, read binding validation, snapshots, indexes
and procedures. Exact UUID/location/schema comparisons remain required. No retry,
cache TTL extension or identity check was removed.

Two independent real SDK catalogs cover external schema evolution (old write must
fail) and rename/recreate (new write must succeed without inheriting old schema).
The complete Paimon owner passed 394 tests, zero failures/errors/skips, plus
Checkstyle. The final `TableControlLifecycleE2ETest` passed all eight tests, zero
failures/errors/skips, in 58.329 seconds through authenticated HTTP SQL, the real
planner, typed v2, Worker/JNI, Native execution and client result transport.
It verifies four PII policies, NULL/short values, query-pipeline reconstruction,
rename, old-name recreation and writes, independent policies and captured-ID drop
cleanup. Pipeline reconstruction is not independent-process crash recovery.
Native sources/artifacts did not change in this repair; the verified packaged
Release JNI SHA-256 remains
`4e6a3e1cacc646d3666c305092ef629d0ab850181f0b1c7313c520fe9196af38`.

## File delivery deadline checkpoint

Provider URL requests are capped at the authorizing grant's remaining whole seconds;
less than one second remaining is rejected before signing. The Worker returns the
bounded issue-time deadline rather than advertising a new hour-long lifetime.
Opened file reads check their original deadline before and after storage reads/skips.
Expiry closes the stream and reports an error; a storage read returning after expiry
cannot deliver that chunk or be mistaken for successful EOF.

Small-data real HTTP, LOCAL, actual S3 signing and MinIO checks passed: 51 tests,
zero failures/errors/skips, including a read that returns only after expiry. The
changed six Java owners also passed Checkstyle. Native sources are unchanged.

This is an expiry boundary, not immediate RBAC revocation: previously issued provider
URLs follow provider semantics, already transmitted bytes cannot be recalled, and an
in-flight blocked storage read relies on its backend timeout before the post-read check.
Autonomous idle-stream cancellation and live revocation still require separate work.
No heavy object/scale test was run.

## File endpoint ownership/static-analysis checkpoint

File requests defensively copy their multipart part receipts and the injected JSON
mapper configuration. External mutation cannot alter a constructed completion request
or switch a live endpoint's decoding rules. Directory snapshot initialization,
validation, release and page reads share the same connection monitor; cancellation
still uses the independently visible active statement. LOCAL staging explicitly
requires a destination parent, with no fallback path.

The 58 focused deadline/HTTP/LOCAL/real S3 signing/MinIO tests passed, with zero
failures/errors/skips. Worker Checkstyle and SpotBugs passed after these repairs.
One narrowly scoped constructor/field filter documents intentional retention of the
factory-owned S3Client: the pinned SDK declares it `@ThreadSafe`, its configuration is
built before borrowing, the accessor never returns it, and the factory owns its pool
and shutdown. This does not suppress getter exposure or unrelated findings. The other
eight reported Worker findings were repaired in source rather than excluded.
The unchanged materialization/planner findings continue to block global static analysis.

## Combined correctness checkpoint — continuation on 2026-10-26

The final combined selection passed all 30 tests, zero failures/errors/skips:
`TableControlLifecycleE2ETest` (8), `FileDirectorySnapshotIntegrationTest` (8),
`ActivationSmokeE2ETest` (3), and `ColumnMaskingE2ETest` (11). This includes actual
Native SQL and activation, plus the PostgreSQL directory component; the latter is
not Native DIRECTORY SQL. The quoted-column regression asserts the rendered
identifier `"ssn"` explicitly instead of expecting the old unsafe bare substitution.

CREATE registration and orphan registration now validate the exact control record
read for mutation. A separate preflight name check cannot protect an interleaving
rename by itself. Neither path may overwrite RENAMING or release either reserved
name; the captured metadata revision still fences later concurrent changes.
The 57 selected service/JDBC/tag regressions passed after this final guard, as did
Metadata Checkstyle/SpotBugs. The complete 830-test Metadata result recorded above
belongs to the earlier post-upgrade checkpoint; it is not relabelled as a fresh
complete reactor run after this final guard.

All remaining implementation/production gates below remain open. This checkpoint
qualifies the listed paths and does not claim M0–M5 or the entire plan complete.

## Resumed implementation and qualification

Current user-directed priority: align architectural correctness and research the
remaining design before heavy qualification. See the [2026-10-25 architecture
review](table-and-file-plane-architecture-alignment-2026-10-25.md). The 100 GiB,
hundred-million-entry, PB-scale and Linux production-environment gates are
**deferred to a dedicated test environment at the user's request**. They remain
open requirements; this checkpoint does not launch them on the development host.
Bounded correctness checks and already-running owner selections may finish.

The catalog deployment identity gap is now implemented. CREATE, ADOPT, orphan
registration and legacy registration capture the authoritative incarnation and
revision. Admission, SQL and metadata read bindings (including cache hits),
governance bindings, statistics, observations and rename/drop completion reject
a changed deployment. The registry also compares the local handle with the
durable descriptor, so a stale peer cannot register authority under a replacement
catalog. Control-layer identity values do not depend on catalog assembly types.

Legacy observation backfill requires both catalog fields to be absent, an
explicit legacy-write allowlist entry and a matching previously registered format
UUID/location. It cannot replace an existing or partial catalog binding. CREATE
under a different confirmed deployment gets a new stable table ID; ADOPT rejects
an old binding until its LINKED registration is explicitly unlinked.

| Check | Observed result | Qualification limit |
|---|---|---|
| Catalog/control/metadata focused tests | 78 passed, including 4 real PostgreSQL deployment-binding tests; no failures, errors or skips | Includes same-UUID catalog replacement, revision changes, stale peer handles, cache rejection and guarded legacy backfill. |
| Coordinator control-plane focused tests | 70 passed across ownership DDL, maintenance, ANALYZE, SHOW and scheduled table selection | Component scope; process-level active-active qualification remains open. |
| HTTP SQL Table Control lifecycle | 7 passed with packaged Release JNI, actual SDK files, PostgreSQL and the prepared real Ollama deployment; no skips | Includes the new catalog replacement case rejecting SELECT, INSERT, DROP, ANALYZE and CALL while retaining the unchanged physical UUID and data. |
| Metadata/coordinator static analysis | Both `verify -Pstatic-analysis -DskipTests` selections passed; zero reported bug instances | Tests ran separately. The analyzer still prints nonblocking missing-lambda-symbol diagnostics. |
| Current Java reactor installation | `-Pnative-runtime -DskipTests install` passed | Compilation/package evidence; refreshes stale local CLI/shuffle artifacts that had contained older SPI classes. |
| Clean metadata owner selection | 822 passed; zero failures, errors or skips; Maven exit 0 | Complete owner evidence for the catalog-fencing snapshot, following the observer-fixture repair. Does not replace the full dependency reactor or process-level qualification. |
| Clean Coordinator owner selection | 3,264 passed; zero failures, errors or skips; Maven exit 0 | Clean compiled outputs plus the actual-authority fixture repairs in `83eadc51`; the later Shuffle/Paimon artifact changes require their own affected checks. |
| Architecture and lightweight gates | Package dependency acyclicity, scoped/full-owner Checkstyle, SPI annotations, E2E truthfulness, documentation references and dead-stub checks passed | The initial control/catalog package cycle was removed before acceptance; these checks do not establish runtime reachability by themselves. |
| Current Release native suite | 4,333 passed; 1 of 4,334 failed with an mTLS `wrong version number` handshake error | The same isolated case subsequently passed once and in 100 diagnostic repetitions. Its intermittent full-suite failure is unresolved and the Release gate remains open. |
| Current macOS ASan native suite | 4,334 passed in 1,985.30 seconds; runner exit 0 | Rebuilt Rust/JNI combination, with `detect_leaks=0` per macOS policy. Linux ASan/LSan and the failed Release gate remain separate requirements. |
| Current isolated Native runtime owner suites | WorkerBootstrap 5; NativeExchangeTransportExternalIntegration 3; WorkerShuffleChannelAdmission 1; CopySourceReadHttp 1 passed, each owner in a separate JVM | All four owners also passed again after installing `a7877791` / `7c50471b` artifacts; zero failures, errors or skips. The downstream classpath contains all 651 canonical SPI classes only in the SPI JAR. |
| Coordinator fixture repairs | 61 tests passed after cleaning compiled outputs | Generated-column fixtures now persist catalog descriptors, routing retains actual Worker identity, and managed write admission registers the connector identity. The clean complete owner result is recorded above. |
| Shuffle library/executable boundary | 46 module tests passed; repeated packaging and standalone configuration-loader smoke passed | The main artifact contains only 75 service classes; `-exec.jar` contains matching bytes for all 651 canonical SPI classes. Installed thin artifact passed all four downstream Native owners (10 tests); the executable remains a separately attached artifact. |
| Paimon connector procedure matrix | 392 module tests passed, including 6 new durable-effect cases; Checkstyle and static analysis passed with zero reported bug instances | `7c50471b` wires the implemented descriptors to connector discovery. Real local SDK effects include compaction, tag retention/delete, rollback after catalog reopen, branches and orphan deletion. Unsupported operations remain unadvertised and fail without changing live rows; SQL CALL authorization is unchanged. Spark/Flink, process-level and final artifact qualification remain separate. |

The first metadata owner attempt selected 822 tests: 818 passed and 4 failed
on an observer fixture that discarded catalog descriptors. Persisting those
descriptors repaired the fixture; the later clean 822-test run passed as recorded
above. The earlier failed report is not treated as a passing run.

The broader dependency reactor was also interrupted while a Docker exec output
stream remained open after its command exited. This is retained as an incomplete
reactor; no test was disabled or timeout raised. The final same-source regression,
sanitizers, isolated Native owners and deployment gates below remain required.

The rebuilt macOS ASan JNI SHA-256 is
`712c1b7cff054908f51b434e6ef5d0ca61a941f81334719486914fc4c97aaeae`.
It uses the Rust archive identified below. The successful ASan report does not
replace the unresolved Release mTLS failure.

The broader Coordinator selection exposed fixtures that discarded durable catalog
state or substituted a synthetic Worker identity; `83eadc51` repairs those
fixtures without weakening admission checks. Cleaning also removed an orphaned
compiled test whose source is absent from the repository. The first incremental
selection remains failed; the subsequent complete clean owner result is recorded
separately above.

`a7877791` separates the Shuffle library JAR from its attached `exec` executable
and updates the service image input. Replacing the main dependency artifact with
a fat JAR had embedded duplicate SPI classes in Java consumers; incremental
packaging could then expose stale contracts ahead of the canonical SPI JAR.

### Declared deletion-vector format boundary

The release scope now explicitly supports uncompressed Puffin footers and
uncompressed DV blobs, with the limits documented in
[Storage backends](../getting-started/catalogs/storage-backends.md#iceberg-deletion-file-compatibility)
and the production sign-off checklist. Optional LZ4 footer compression is a
deliberate unsupported format boundary. Compressed `deletion-vector-v1` blobs
are prohibited by the [Iceberg Puffin specification](https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type)
and must remain rejected; they are not missing DV compression functionality.
External-writer and final-artifact acceptance remain open.

## Table Control Plane

### Root causes repaired

| Area | Root cause and resulting behavior | Commit |
|---|---|---|
| Physical deletion vectors | The reader expected a synthetic Puffin header and a 32-bit bitmap instead of Iceberg SDK bytes. It could choose the wrong blob and lose positions above 2^32. The reader now validates PFA1 framing, blob bounds/type/file identity, length, magic, CRC, bitmap ordering and cardinality; selection uses the scanned data file and preserves 64-bit positions. | `199acb4d` |
| Delete execution | Encoded vector children were treated as flat; absent equality columns could produce incomplete filtering. Decode/flatten the required children and reject unqualified equality deletes before file I/O. The public equality-read capability remains false. | `199acb4d`, `662e15e2` |
| Table identity and observations | Stale observations could update a newer control revision; cache projections omitted control identity/version; some mutation paths did not fence physical identity. Publish registry/version history atomically, enforce CAS and identity, and invalidate the corresponding projections. | `b36a83d7` |
| Reconciliation | Discovery failures could be confused with absence, and stale/orphan recovery lacked sufficient identity checks. Preserve unavailable observations, restore only matching physical identity, and quarantine replacements. | `b36a83d7` |
| Rename | Name-based identity changes could invalidate Paimon ownership or expose an incomplete transition. Reserve RENAMING, carry the stable table ID, refresh physical identity after the rename, and reject reads during an unresolved transition. | `b36a83d7` |
| Statistics | Collection could race a replacement or a control revision update. Capture and recheck identity, version and control revision before publishing ANALYZE results. | `b36a83d7` |
| Commit ambiguity | Iceberg verification could treat an unproven outcome as failure; an outer Paimon retry could republish after the SDK had committed. Preserve UNKNOWN and let the SDK own its commit retry protocol. | `662e15e2` |
| Recovery SQL | Attempt reconciliation selected commit-evidence fields without joining the evidence table. Restore the LEFT JOIN and verify the actual persisted evidence. | `960cd5fc` |
| Maintenance | CALL bypassed the control-plane admission path. Reentrant holder IDs also allowed concurrent attempts on one coordinator, and a long operation could outlive its lease. Route CALL through ownership/RBAC/policy checks, allocate a holder per attempt, renew the lease and verify ownership before execution and before reporting success. | `dd34c0a9` |
| Maintenance outcome | A rejection after connector execution had begun could be recorded as a harmless skip. Such failures now remain FAILED; loss of the lease does not imply that physical work was rolled back. | `dd34c0a9` |
| Native E2E ownership fixtures | Shuffle/COPY fixtures did not bind the real Worker process identity before admission. Bind the actual owner throughout the fixture chain. | `3850e292` |
| Overwrite regression fixture | A static-partition INSERT supplied the partition column in VALUES, violating the documented SQL contract. Supply only non-partition columns while retaining assertions that the selected partition is replaced and other partitions survive. | `abd0ddf3` |
| Lease regression fixture | A remaining-TTL assertion included client scheduling and connection latency. Assert that the database grant timestamp follows the blocking transaction boundary and retains its full duration. | `922fdf1b` |
| Rust protocol artifact drift | The local Lance archive still used generated messages without Worker execution identity. Rust dropped that field, so a real Lance INSERT produced a transaction reference whose attempt-path hash failed Java validation. Require a source/header/archive/target provenance receipt at CMake configure and build time; rebuild the Rust archive before relinking JNI. Docker now stages the SDK preparation script and patch before its Rust build. | `4f26149e` |
| Isolated E2E Native classpath | Selecting only the E2E module did not activate the Worker project's profile transitively, leaving JNI absent despite `-Pnative-runtime`. Declare the classifier dependency directly in the E2E profile. | `4f6a45d4` |

### Evidence obtained

These results refer to isolated source/runtime snapshots used during this review.
Concurrent changes to CLI packaging, AI execution and FILE/Volume/Stage were
integrated into the branch afterward. The final combined revision must be
qualified again; earlier green results must not be represented as that result.

| Check | Observed result | Qualification limit |
|---|---|---|
| Native Release suite after DV repair | 4,333 tests passed | One subsequently added factory test was absent from this compiled test binary. |
| Native deletion-vector focused suite after forcing the test target to rebuild | 30 tests passed, including multiple-blob selection with a 64-bit row offset | Complements the preceding full run; this is not a single 4,334-test full run. |
| `TableControlLifecycleE2ETest` | 6 passed through HTTP SQL, real native execution, PostgreSQL control records, Iceberg SDK files and Paimon rename | SDK external writer, not Spark/Flink. Store reconstruction, not coordinator process restart. |
| Recovery focused integration | 9 passed: HTTP lineage recovery and materialization repository | Real PostgreSQL and the pinned Marquez 0.50.0 application on a native ARM runtime. |
| `JdbcDistributedLeaseLockClockIntegrationTest` after assertion repair | 9 passed | Actual PostgreSQL row/conflict waits and delayed writes. |
| `WriteDmlTransactionE2ETest$InsertOverwrite` after SQL repair | 1 passed | Actual static partition overwrite. |
| Java static analysis | protocol, metadata, Iceberg, Paimon and coordinator passed | Before the final combined-revision rebuild. |
| Combined Java reactor | Clean-snapshot install passed, followed by install with `native-runtime` | Compilation/package evidence only. Isolated E2E subsequently exposed the transitive-profile problem above; installing Worker with the profile alone did not fix it. |
| Combined-snapshot metadata focused integration | 80 passed across control registry, policies, statistics, database lease clocks, actual HTTP lineage recovery and materialization evidence | Includes a successful Marquez rerun after reducing concurrent load; full metadata/reactor qualification remains open. |
| Combined-snapshot coordinator focused tests | 71 passed across maintenance SPI/source/execution, dispatch properties, table extraction, permissions and CALL routing | Focused coverage; does not replace process-level active-active and external-writer qualification. |
| Lance archive provenance | 4 tooling tests passed; actual stale archive rejected by both the verifier and CMake configuration; changing a protocol after configuration also blocks the next build | Rust rebuilt with a valid receipt and Release JNI/test/fixture targets relinked successfully. |
| Rebuilt Rust/JNI affected Native suite | 38 tests passed: Lance commit execution and deletion-vector reader/factory/filter | New combination; full Release and sanitizer regression still required. |
| Rebuilt Rust/JNI HTTP SQL acceptance | 9 passed: NativeDatasetControlRoute 2, TableControlLifecycle 6, static-partition overwrite 1; no skips | Confirms the formerly failing real Lance INSERT/read, actual SDK DV reads and control-plane lifecycle on the new JNI. Full deployment matrix remains open. |
| Isolated Native runtime owner suites | WorkerBootstrap 5; NativeExchangeTransportExternalIntegration 3; WorkerShuffleChannelAdmission 1; CopySourceReadHttp 1 passed | These 10 results precede the latest deletion-vector JNI build. |
| macOS ASan full suite | 4,334 tests passed in 2,308.42 seconds | Exact pre-rebuild JNI/Rust artifact; `detect_leaks=0` per macOS policy. New Rust/JNI and Linux ASan/LSan remain separate gates. |
| Broad Java and SQL E2E suites | Stopped with the failures below after their artifacts were superseded | Incomplete and not green; partial successes do not qualify the suite. |
| Canonical Linux production image | Build incomplete | A local macOS runtime cannot qualify the Linux image. |

The initial native and HTTP SQL tests using genuine SDK Puffin files failed on
the old header parser before the repair. The focused native and six-case SQL
suite subsequently passed. No capability was enabled based on synthetic data.

The broader metadata run exposed the lease test timing issue fixed above and a
Marquez startup HTTP read timeout. The latter had passed in the focused run;
resource contention is suspected, but a successful serialized rerun is still
required. The broad E2E run exposed the static-partition fixture fixed above.
The broad E2E run also found a real Lance INSERT failure caused by the stale Rust
protocol archive described above. Earlier Release and ASan results must therefore
be retained as evidence for their exact old artifact, not promoted to validation
of the rebuilt Rust/JNI combination.

The successful rebuilt Rust archive SHA-256 is
`786a171a8eb8ba416a82652c3502fe3c4fd83ea360c672bce96e1a47d367ee98`;
the relinked macOS Release JNI SHA-256 is
`4e6a3e1cacc646d3666c305092ef629d0ab850181f0b1c7313c520fe9196af38`.
The new 38-test native and 9-test SQL results belong to these artifacts. The Java
snapshot contains the combined `abd0ddf3` source, the lease-test repair
`922fdf1b`, and the isolated E2E profile repair `4f6a45d4`; later concurrent FILE
changes on the shared branch are not covered by that snapshot's results.

No timeout was raised, test disabled, or new sanitizer suppression added to make
these failures disappear.

### Remaining production blockers and unimplemented scope

| Plan area | Remaining work | Required acceptance evidence |
|---|---|---|
| P0 final regression | Complete a clean build and the affected/full Java reactor against the final combined revision; rerun failed suites and all four isolated Native owner suites with the final JNI artifact. | Matching source/artifact hashes, complete reports, no unresolved failure or skipped mandatory gate. |
| P0/P1 catalog deployment identity qualification | Registration, adoption, guarded legacy backfill and admission comparisons are implemented and covered by the resumed checkpoint above. Complete final combined-revision and process-level qualification. | Retain same-UUID catalog replacement/revision and stale-peer PostgreSQL/HTTP SQL evidence in the final regression. |
| P1 stable governance identity | Canonical tags and authored/derived column-mask reads now use stable table IDs, including deferred raw-read authorization and captured-ID cleanup. Authored row-policy reads now share the stable UUID in foreground/background paths. Quality definitions/results now use stable ownership with permanent rule IDs; actual quality execution, column-schema identity and process recovery qualification remain open; do not equate this classification checkpoint with all governance being complete. | ADOPT → tag/mask/quality/audit → rename → restart → UNLINK; external drop/recreate cannot inherit stale policy. |
| P1 external writers | Add/run actual Spark local Iceberg writes on MinIO with JDBC/REST catalogs. | Append, position delete, DV, overwrite and equality-delete fail-closed results compared with Spark. |
| P2 recovery | Crash/ambiguous rename persists the source intent and reserves both names in RENAMING; it fails closed across store restart. Durable automatic reconciliation of physical rename and governance projections is not implemented. | Crash injection before/after physical rename and every projection step converges without granting authority to a replacement. |
| P2 lifecycle/governance integration | Complete combined-revision validation of automatic maintenance leadership, durable audit, tags/masks, write-quality gates, statistics consumed by planning, UNKNOWN recovery and orphan cleanup. | Actual SQL results and durable effects across coordinator process restart and leadership transfer. |
| P2 Paimon external writer | Add/run Flink mini-cluster fixtures for primary key, sequence groups, aggregation and partial updates. | Flink-produced data remains correct and respects MANAGED/external ownership boundaries. |
| P3 equality deletes | Full equality-delete semantics remain unimplemented/unqualified; capability must stay false. | Spark oracle for equality/position/DV mixtures, null/type/schema-evolution and snapshot/sequence applicability. |
| P3 external lineage | Complete the snapshot/audit-log reconstruction and table/column lineage depth audit. | Queryable external lineage with durable ordering and restart recovery. |
| P3 active-active | Exercise two real coordinators plus an independent writer, not only store instances or threads. | CAS, lease loss, version recheck, conflicting commits and restart tests show no duplicate/lost data or false success. |
| P3 deployment matrix | Finish canonical same-source Linux image, Linux ASan/LSan, S3/MinIO and REST/Hive/JDBC catalog qualification. | Image/source provenance and all mandatory CI/release gates pass. |
| Format boundary qualification | The supported DV format is now documented as uncompressed footers and uncompressed blobs. Optional compressed footers remain unsupported; compressed DV blobs violate the specification and stay rejected. | Verify the declared limit against actual external writers and the final shipped artifact; retain explicit rejection evidence and do not advertise unrestricted Puffin encoding support. |

Paimon procedures that remain unsupported must continue to fail explicitly and
remain excluded from discovery. The current local SDK matrix is now verified
above; it does not authorize otherwise rejected SQL CALL mutations or close
external-writer and process-level qualification.

## Multimodal file plane

### Implemented and wired

- Volume and Stage definitions, immutable identity, DDL, metadata persistence,
  grants, authorization and audit; SQL `LIST` and `REMOVE`; managed roots retain
  identity across rename and are not reused by a new object with the same name.
- Coordinator-issued short-lived, credential-free file operation descriptors;
  Worker LOCAL/S3 streaming access, metadata, listing, conditional deletion,
  ranged reads, HTTP HEAD, version conditions and provider presigned URLs.
- Stable Stage URLs resolve the immutable Stage identity and recheck current
  authorization on every GET/HEAD before redirecting to a short-lived Worker URL.
  The previously issued Worker URL remains usable until its own expiry.
- Manual Stage refresh publishes a durable directory index atomically only after
  a complete listing. Refresh attempts are fenced. A Worker-side PostgreSQL page
  reader supports bounded pages, a consistent snapshot, cancellation and deadlines.
  This page reader is not wired to a Native SQL Directory operator.
- Multipart sessions now have a signed 24-hour absolute expiry independent of
  short-lived request grants. Tokens bind the complete storage scope and path;
  renewal requires a fresh authorized grant and can continue on another Worker.
  Expired cleanup retains ownership on failure for a subsequent retry.
- LOCAL multipart parts use content-based SHA-256 ETags and atomic replacement.
  Truncated/oversized/interrupted writes preserve the previously accepted part.
  Completion checks actual bytes against the client's part ETags and sizes before
  publication; rejected validation preserves the object and retryable parts.

The behavior and API details are in
[Volumes, Stages and file access](../getting-started/catalogs/volumes-stages-files.md).

### Validation evidence

The focused Maven reactor selection completed with **91 tests, zero failures,
errors or skips**, covering the following classes against real PostgreSQL,
production HTTP resources and the packaged Native library where applicable:

- `JdbcFileSpaceRegistryTest` (12), `JdbcFileDirectoryIndexTest` (8),
  `JournalSqlOperationTest` (1), `JdbcQueryLineageOutboxIntegrationTest` (9),
  `JdbcCopyFrozenInputLineageTest` (5).
- `FileSpaceAccessServiceTest` (10), `FileUrlServiceTest` (4),
  `WorkerFileClientTest` (6), `StageDirectoryRefreshServiceTest` (3),
  `FileSpaceObjectStoreTest` (5).
- `StageFileUrlHttpIntegrationTest` (6), `FileDirectorySnapshotIntegrationTest` (8),
  `FileSpaceDdlE2ETest` (5), `FileSpaceControlPlaneContractE2ETest` (3),
  `ActivationSmokeE2ETest` (3), `SpiWiringVerificationTest` (3).

The subsequent multipart changes passed isolated JDK 17/JUnit checks using the
reactor dependency classpath, with zero failures or skips:

- `FileSpaceFileResourceTest`: 19; `S3ObjectStoreAccessorMinioTest`: 8 against a
  real MinIO container; `LocalObjectStoreAccessorTest`: 16;
  `FileSpaceHttpReadTest`: 20; `FileSpaceMultipartHttpTest`: 1.
- The new two-Worker renewal test fails against the pre-fix resource because the
  origin Worker deleted staged parts after request-grant expiry. It passes after
  the session lifetime repair.
- Three LOCAL retry/content regression cases fail against the pre-fix accessor
  and pass after the integrity repair.
- Scoped Checkstyle for both changed production classes passes. Earlier scoped
  file-plane Checkstyle, dead-stub/connectivity, SPI annotation and E2E
  truthfulness checks passed. These are not a passing repository-wide static gate.

The two HTTP Worker endpoints above run in one JVM. This establishes routing and
shared-storage behavior, not independent-process recovery or distributed qualification.

The broader default-profile metadata/coordinator/worker owner reactor was
interrupted during quota wrap-up with exit 130. Metadata completed 514 tests with
zero failures, errors or skips; this profile excludes integration-tagged metadata
tests. Earlier dependency modules also completed successfully. The run had not
reached the Coordinator and Worker owner suites. This is an incomplete reactor,
not a passing broader gate; rerun the affected owners and full release profile.
The earlier owner run stopped with two metadata errors; their root causes
(journal double cleanup and an empty migration update advancing the authorization
revision) were fixed, and the affected focused tests subsequently passed.

### Remaining work against the design

| Work package | Remaining implementation / acceptance |
|---|---|
| M0: definitions and governance | Finish the full cross-tenant, external-location privilege, audit and migration acceptance matrix; retain real database and authenticated HTTP evidence. Existing passing cases do not cover every combination. |
| M1: transfers and URLs | SQL URL functions and SQL/client PUT/GET surfaces; scoped URLs and S3 AssumeRole/STS credentials; durable multipart ownership/recovery and autonomous reclamation; S3 large-object and 100 GiB multipart acceptance. The LOCAL 5 GiB HTTP gate is recorded separately below and does not qualify these other backends or multipart sizes. |
| M2: queryable files and ingestion | Wire `DIRECTORY(@stage)` through planner → typed v2 → Worker → JNI → Native with the exact six columns; implement URI `COPY INTO`, `read_files` with the eight requested formats, external-table DDL/readers, write-through index updates and actual `FILE MANAGED` copy/publication. FILE modifiers and index/page-reader foundations are not those completed execution paths. |
| M3: Iceberg v3 and AI | Verify every requested Iceberg v3 feature and AI function against the latest implementation and real providers, including FILE authorization, modality/size limits and exact SQL semantics. Changes and tests in other workstreams are not automatically qualification of the design's full matrix. |
| M4: lifecycle and operations | Managed DROP physical-file semantics, external-file preservation, automatic refresh, events/Inventory, durable restart/checkpoint recovery, per-tenant/space byte and concurrency quotas, backlog/coverage and latency metrics, documented operational limits. Manual refresh and opportunistic multipart cleanup do not satisfy this package. |
| M5: production scale and clients | OSS/GCS/ADLS implementations and consistency tests; 100-million-entry directory qualification; real client REST/JDBC/CLI E2E; fault/restart/multi-process tests and deployment/runbook verification. |
| Release acceptance | Full affected Java/Native/JNI chain on the final source, required sanitizer checks, independent-process/multi-node failures and release full regression. Native smoke tests do not replace these gates. |

### Reproducing the large-object gate

`FileSpaceLargeObjectIntegrationTest` transfers a real 5 GiB LOCAL object through
HTTP, compares full upload/download SHA-256, checks HEAD length, verifies bytes
across the 4 GiB boundary and rejects a stale ETag. Allow at least 6 GiB of free
scratch space and several minutes; temporary files are removed by JUnit. This is
not a sparse-file or mocked byte-count check and does not qualify S3 large objects
or 100 GiB multipart.

```bash
mvn -pl pangrova-worker -am -Pfull-regression \
  -Dtest=FileSpaceLargeObjectIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

The initial standalone harness passed on 2026-10-25 with a 256 MiB Java heap,
transferring and downloading all 5,368,709,120 bytes. Upload and download SHA-256
were `7535f0d34d4af8a46292dbb5958215503907cd0bd269ada090dfc59ff3896d6f`.
HEAD, the range crossing 4 GiB, and the stale-ETag 412 check passed; its temporary
object was removed. The final JUnit integration test also passed: one test,
zero failures/errors/skips, 267 seconds with a 256 MiB heap. Its position-dependent
pattern includes high offset bits, so a 32-bit range-offset wrap cannot return
matching bytes. JUnit removed the temporary 5 GiB object after the test.

## Shared production gates and continuation order

The P0–P3 and M0–M5 matrices above retain their workstream-specific acceptance
criteria. Final-source regression, artifact provenance, sanitizer validation,
process-level recovery and deployment qualification are shared release gates;
focused or historical results do not close them.

1. Complete architecture alignment before extending the file execution path, as
   detailed in the linked 2026-10-25 review: immutable directory generations,
   durable uploads and UNKNOWN recovery, separate authorization/ownership/read
   protection, and stable governance projections. Heavy capacity and Linux
   production-environment testing is deferred to the dedicated environment.
2. Freeze the next combined branch revision and complete the affected
   native/Java chain. Release JNI, its classifier and a new runtime manifest have
   been rebuilt; the focused SQL acceptance passed. Preserve reports and hashes
   from previous artifacts separately. Complete the remaining Worker-wide and full
   release profile gates against the final source. The clean metadata / Coordinator
   owners, macOS ASan and all four
   isolated runtime owners have now passed for the checkpoints recorded above;
   do not rerun unchanged inputs merely to accumulate green reports.
3. Reproduce and correct the unresolved Release mTLS failure and dependency-reactor
   Docker exec stream hang. Run any newly affected owner in a separate JVM;
   preserve failed evidence until a relevant correction is validated.
4. Complete stable-ID governance projections and process-level qualification of
   catalog deployment fencing, then add actual crash/restart and active-active
   acceptance fixtures.
5. Implement the Native Directory execution slice and then ingestion and FILE
   managed publication, following the execution boundary below.
6. Add the Spark/Flink external-writer matrix and the equality-delete oracle.
   Complete the remaining file-plane governance, transfer, backend, scale,
   lifecycle, AI and client acceptance matrices before closing their gates.
7. In the dedicated environment, build and qualify the canonical Linux image,
   Linux sanitizers and full release regression against the final implementation.
   Only then reassess production readiness.

### File-plane execution boundary

The next main vertical slice is the Native Directory execution path. Resolve and
authorize the immutable Stage reference in the control plane, propagate a typed
binding and publish capability only when the Worker/Native path is actually wired.
First implement the reviewed immutable generation/Manifest boundary; do not wire
the Worker JDBC reader into production SQL. Construct the six columns in Native
and verify SELECT, predicates and joins with real directory snapshots and
authorization changes. Do not return relational query rows from Coordinator Java or register a placeholder table function.

Then connect ingestion and FILE managed writes to the existing fenced publication
mechanisms before claiming the design's first complete upload → DIRECTORY → COPY
→ FILE/AI query loop. Keep the remaining size, scale, backend and recovery gates
explicit until executed successfully.

Do not mark either original plan complete merely because targeted tests pass.
