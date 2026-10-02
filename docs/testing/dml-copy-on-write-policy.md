# Copy-on-write DML policy regression

`DmlCopyOnWritePolicyE2ETest` exercises authenticated public SQL against the real
Iceberg connector, PostgreSQL authority stores and Worker/JNI execution. It does
not fabricate output handles, disable assertions, or skip unavailable runtime.

The fixture writes five physical rows through Native INSERT before installing
an authoritative direct role, schema grant, `tenant=1` row filter, and constant
zero score mask. It asserts that the same HTTP actor sees only the three
authorized rows and masked scores before executing DML.

The five scenarios cover UPDATE and DELETE with a masked-score predicate and a
nullable unmasked predicate, UPDATE and DELETE without WHERE, and conditional
MERGE matched UPDATE. After each operation the test deletes only its persistent
row/mask policies, retaining the same actor, direct role and grants, then queries
all original physical columns through public SQL. Exact row comparisons verify
outside-policy survivors, unassigned raw scores, original NULL values, stable
non-contiguous IDs and only the intended changed columns.

Run after the latest JNI package and Java reactor install, using the repository
build pipeline's E2E environment and fail-on-no-tests settings:

```sh
mvn -pl pangrova-testing/pangrova-test-e2e -Pe2e test \
  -Dtest=DmlCopyOnWritePolicyE2ETest -DfailIfNoTests=true \
  -Dsurefire.failIfNoSpecifiedTests=true
```

This source checkpoint is not an execution PASS. Central validation must record
all five scenarios executed, zero failures/errors/skips, and the complete latest
source Native/JNI chain. A focused test cannot replace that full-chain gate.

## Pre-allocation and policy-observation checks

`QuerySchedulingEngineWriteAdmissionTest` verifies the sole scheduling boundary
with a real Iceberg HadoopCatalog, production output-handle codec and actual
NativeWriteCommitTracker. The test-only metadata subclass counts real SDK
allocations and injects an explicit allocation failure; it never returns a
fabricated Native fragment. Missing authority (ordinary and generated tables),
real PostgreSQL grant revocation, absent compilation evidence, another table's
declaration and invalid partitions must all fail before allocation/registration.
Successful INSERT versus overwrite modes preserve original SQL actions,
including UPDATE-only MERGE, exact static partitions and serialized output
identity. Repeated query registration observes current authority again without
reallocating; changed subject, tenant, action or actual SDK schema is rejected.
The mandatory `query.task.write.WriteAllocationIntent` additionally binds SQL
write mode and the immutable, key-order-independent static partition map.

`QueryWriteReservationConcurrencyTest` exercises bounded concurrent scheduling,
one real SDK allocation, current authority checks on both callers, mode/scope
changes, retry after a definitive pre-output failure, and cancellation after an
allocation starts. Pending allocation uses a future reservation; SDK I/O happens
outside map/monitor locks. A cancelled allocation with a known output remains
quarantined and inspectable instead of being reused or claiming an unimplemented
abort. `WriteAllocationIntentTest` verifies exact values and immutable scope.

`WritePolicyObservationTest` uses the actual compiler and PostgreSQL direct-role,
grant, row-policy and mask stores. It verifies INSERT VALUES has no source reads,
UPDATE/DELETE preserve raw physical target inputs while observing policies,
JOIN sources carry complete compiled scan columns and explicit policy absences,
and UPDATE-only MERGE does not invent INSERT/DELETE privileges. A source-INSERT
case in the scheduling test additionally resolves SDK tables through the real
metadata bootstrap and forwards those exact protected observations to the
five-field authority request. These are component/authority tests, not SQL
Worker/Native E2E results; generated-model execution is not claimed by their
negative pre-allocation cases.

The central closest-test command, after reactor test compilation, is:

```sh
mvn -pl pangrova-coordinator -am test \
  -Dtest=QuerySchedulingEngineWriteAdmissionTest,QueryWriteReservationConcurrencyTest,WriteAllocationIntentTest,WritePolicyObservationTest \
  -DfailIfNoTests=true -Dsurefire.failIfNoSpecifiedTests=false
```

The central owner must verify all named coordinator suites actually executed;
the reactor's upstream modules may contain neither named suite.

## Iceberg static overwrite scope

The sole Iceberg output-handle codec is version 3. Its exact serialized handle
contains the static partition map, table UUID, captured schema/spec and base
snapshot; the existing durable binding/fingerprint protects those bytes during
recovery. There is no old-version decoder or forwarding constructor.

Static overwrite supports typed identity-partition source columns, including
partial partition specifications. The SDK row-filter overwrite clears all
matching partitions even if the new result is empty or contains fewer partition
values; added files must satisfy the captured filter. Unscoped dynamic overwrite
retains partition replacement semantics. Unscoped empty overwrite clears the
whole table. Unknown/non-partition columns, transform-only partition columns,
unrepresented null/binary values and invalid typed literals reject before
dispatch; these unsupported options are not claimed as implemented. Schema/spec
changes since static allocation reject instead of rebinding the scope.

`IcebergStaticOverwriteScopeTest` writes and reads actual SDK Parquet files,
verifies outside-scope survivors and NULL payloads, empty/partial overwrites,
out-of-scope file rejection, exact newest-codec recovery, replay after another
append and pre-allocation validation. This is an SDK commit/recovery component
test, not a Native/authentication E2E. Central validation must execute it with
zero skips alongside the existing Iceberg atomic/deletion-mode suites.
