package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.copy.PrepareCopySourceRequest;
import org.pangrova.protocol.rest.copy.PrepareCopySourceResponse;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.SourcePreparationRequest;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.resource.memory.RootMemoryPool;
import org.pangrova.resource.tenant.TenantResourceTracker;
import org.pangrova.worker.copy.CopyStageLeases;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exact common admission authority with real node and tenant reservation owners. */
class SourcePreparationControlTest {
    private static final long HEAP = 1024 * 1024;
    private static final CopySourceReadHttp.StagedSource PROOF = new CopySourceReadHttp.StagedSource(
            "version-1", "s3://managed/copy/object", "a".repeat(64), 5, "b".repeat(64));
    @TempDir Path directory;

    @Test
    void unknownSealPreventsLatePreparationWithoutAnySourceIo() throws Exception {
        var calls = new AtomicInteger();
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            calls.incrementAndGet();
            return PROOF;
        })) {
            var request = request("sealed");
            assertThat(fixture.manager.sealAdmission(request.admissionSeal())).isPresent();
            assertThatThrownBy(() -> fixture.control.execute(request)).isInstanceOf(TaskAdmissionException.class);
            assertThat(calls).hasValue(0);
            fixture.assertReleased();
        }
    }

    @Test
    void inFlightSealRetainsResourcesUntilActualProducerExit() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            budget.reserveRead(5);
            entered.countDown();
            await(release);
            return PROOF;
        })) {
            var request = request("running");
            var running = executor.submit(() -> fixture.control.execute(request));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.control.seal(request.admissionSeal())).isEmpty();
            assertThat(fixture.node.getUsedBytes()).isEqualTo(HEAP);
            assertThat(fixture.quota.getTenantActiveQueryCount("tenant")).isEqualTo(1);
            assertThat(fixture.quota.getTenantMemoryUsage("tenant")).isEqualTo(HEAP);
            release.countDown();
            var result = running.get(5, TimeUnit.SECONDS);
            assertThat(result.getError().getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_CANCELLED);
            assertThat(result.getUsage().getObjectStoreReadBytes()).isEqualTo(5);
            assertThat(fixture.control.seal(request.admissionSeal())).isPresent();
            fixture.assertReleased();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void failedUnlinkRetainsExactDraftAndReservationsUntilCleanupRetry() throws Exception {
        Path draft = Files.createDirectory(directory.resolve("draft"));
        Path barrier = Files.writeString(draft.resolve("barrier"), "retained");
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            budget.ownDraft(draft);
            budget.reserveRead(5);
            throw new IOException("source interrupted");
        })) {
            var request = request("cleanup");
            var result = fixture.control.execute(request);
            assertThat(PrepareCopySourceResponse.from(result).resultFor(request).getIoJoined()).isFalse();
            assertThat(result.getUsage().getObjectStoreReadBytes()).isEqualTo(5);
            assertThat(fixture.control.seal(request.admissionSeal())).isEmpty();
            assertThat(fixture.node.getUsedBytes()).isEqualTo(HEAP);
            assertThat(fixture.quota.getTenantActiveQueryCount("tenant")).isEqualTo(1);
            Files.delete(barrier);
            assertThat(fixture.control.seal(request.admissionSeal())).isPresent();
            assertThat(draft).doesNotExist();
            fixture.assertReleased();
        }
    }

    @Test
    void completedReplayReauthorizesWithoutRepeatingSourceIoOrDiscardingUsage() throws Exception {
        var calls = new AtomicInteger();
        var allowed = new AtomicBoolean(true);
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            calls.incrementAndGet();
            budget.reserveRead(5);
            return PROOF;
        }, (nonce, scope, proof) -> {
            if (!allowed.get()) throw new org.pangrova.spi.security.AccessDeniedException("revoked");
            return PROOF;
        })) {
            var request = request("replay");
            var result = fixture.control.execute(request);
            assertThat(fixture.control.execute(request)).isEqualTo(result);
            allowed.set(false);
            var denied = fixture.control.execute(request);
            assertThat(PrepareCopySourceResponse.from(denied).resultFor(request).getError().getErrorClass())
                    .isEqualTo(ErrorClass.ERROR_CLASS_POLICY_DENIED);
            assertThat(denied.getUsage()).isEqualTo(result.getUsage());
            assertThat(calls).hasValue(1);
            fixture.assertReleased();
        }
    }

    @Test
    void terminalTransportFailureIsRetainedAndChangedGrantIsRejected() throws Exception {
        var calls = new AtomicInteger();
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            calls.incrementAndGet();
            budget.reserveRead(5);
            throw new IOException("backend failure");
        })) {
            var request = request("failure");
            var failed = fixture.control.execute(request);
            assertThat(failed.getError().getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_TRANSPORT);
            assertThat(fixture.control.execute(request)).isEqualTo(failed);
            var changed = PrepareCopySourceRequest.from(request.preparation().toBuilder()
                    .setResourceGrant(request.preparation().getResourceGrant().toBuilder().setJvmHeapBytes(2 * HEAP))
                    .build(), request.sourceRead());
            assertThatThrownBy(() -> fixture.control.execute(changed)).isInstanceOf(TaskAdmissionException.class);
            assertThat(calls).hasValue(1);
            fixture.assertReleased();
        }
    }

    @Test
    void partialNodeReservationRollsBackTenantSlotBeforeAnyIo() throws Exception {
        var calls = new AtomicInteger();
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            calls.incrementAndGet();
            return PROOF;
        })) {
            long held = fixture.node.tryReserve(4 * HEAP - 10);
            try {
                assertThatThrownBy(() -> fixture.control.execute(request("capacity")))
                        .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("memory");
                assertThat(fixture.node.getUsedBytes()).isEqualTo(held);
                assertThat(fixture.quota.getTenantActiveQueryCount("tenant")).isZero();
                assertThat(calls).hasValue(0);
            } finally {
                fixture.node.free(held);
            }
            fixture.assertReleased();
        }
    }

    @Test
    void fatalProducerFailureStillRetainsRealCleanupOutcome() throws Exception {
        Path draft = Files.createFile(directory.resolve("fatal-draft"));
        var failure = new AssertionError("producer fatal");
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            budget.ownDraft(draft);
            budget.reserveRead(5);
            throw failure;
        })) {
            var request = request("fatal");
            assertThatThrownBy(() -> fixture.control.execute(request)).isSameAs(failure);
            assertThat(fixture.control.seal(request.admissionSeal())).isPresent();
            var replay = fixture.control.execute(request);
            assertThat(replay.getError().getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_INTERNAL);
            assertThat(replay.getUsage().getObjectStoreReadBytes()).isEqualTo(5);
            assertThat(draft).doesNotExist();
            fixture.assertReleased();
        }
    }

    @Test
    void concurrentExactDeliveriesShareOneSourceAndOnePhysicalReservation() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(2);
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            calls.incrementAndGet();
            budget.reserveRead(5);
            entered.countDown();
            await(release);
            return PROOF;
        })) {
            var request = request("duplicate");
            var first = executor.submit(() -> fixture.control.execute(request));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var second = executor.submit(() -> fixture.control.execute(request));
            assertThat(fixture.node.getUsedBytes()).isEqualTo(HEAP);
            assertThat(fixture.quota.getTenantActiveQueryCount("tenant")).isEqualTo(1);
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(second.get(5, TimeUnit.SECONDS));
            assertThat(calls).hasValue(1);
            fixture.assertReleased();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void tenantHeapLimitRejectsBeforeNodeReservationOrSourceIo() throws Exception {
        var calls = new AtomicInteger();
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            calls.incrementAndGet();
            return PROOF;
        })) {
            fixture.tracker.setQuota("tenant", new org.pangrova.resource.tenant.TenantResourceQuota(HEAP - 1, 2));
            assertThatThrownBy(() -> fixture.control.execute(request("tenant-heap")))
                    .isInstanceOf(TaskAdmissionException.class).hasMessageContaining("tenant quota");
            assertThat(calls).hasValue(0);
            fixture.assertReleased();
        }
    }

    @Test
    void capacityRefusalBeforeIoIsTerminalAndReleasesItsPhysicalReservation() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var threads = new java.util.concurrent.CopyOnWriteArrayList<Thread>();
        var executor = Executors.newFixedThreadPool(8, callback -> {
            var thread = new Thread(callback, "preparation-capacity-test");
            threads.add(thread);
            return thread;
        });
        try (var fixture = new Fixture((nonce, scope, budget) -> {
            entered.countDown();
            await(release);
            return PROOF;
        })) {
            try {
                var request = request("producer");
                var deliveries = new java.util.ArrayList<java.util.concurrent.Future<
                        org.pangrova.protocol.v2.SourcePreparationResult>>();
                deliveries.add(executor.submit(() -> fixture.control.execute(request)));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                for (int index = 1; index < 8; index++) {
                    deliveries.add(executor.submit(() -> fixture.control.execute(request)));
                }
                org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).until(() ->
                        threads.size() == 8 && threads.stream()
                                .allMatch(thread -> thread.getState() == Thread.State.TIMED_WAITING));
                var refused = fixture.control.execute(request("capacity-refused"));
                assertThat(refused.getIoJoined()).isTrue();
                assertThat(refused.getError().getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_ADMISSION);
                assertThat(refused.getUsage().getObjectStoreReadBytes()).isZero();
                assertThat(fixture.node.getUsedBytes()).isEqualTo(HEAP);
                assertThat(fixture.quota.getTenantActiveQueryCount("tenant")).isEqualTo(1);
                release.countDown();
                for (var delivery : deliveries) assertThat(delivery.get(5, TimeUnit.SECONDS).hasSource()).isTrue();
                fixture.assertReleased();
            } finally {
                release.countDown();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new IOException("producer release timed out");
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException(failure);
        }
    }

    private static PrepareCopySourceRequest request(String taskId) {
        long deadline = System.currentTimeMillis() + 60_000;
        var scope = new CopySourceReadHttp.Scope("tenant", "user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1, "s3://incoming/file.parquet",
                "s3://managed/copy", "a".repeat(64), "query", taskId, "coordinator", "test", 1, 1, deadline);
        var command = SourcePreparationRequest.newBuilder().setProtocol(CanonicalTaskRequests.protocolVersion())
                .setAttempt(org.pangrova.protocol.v2.TaskAttemptId.newBuilder().setQueryOrJobId("query").setTaskId(taskId)
                        .setPlanEpoch(1).setFencingToken(1L << 32 | 1L).setControlEpoch(1)
                        .setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                                .setOwnerId("coordinator").setOwnerIncarnation("test").setFencingToken(1)))
                .setSecurity(org.pangrova.protocol.v2.SecurityContext.newBuilder().setTenantId("tenant").setPrincipal("user"))
                .setSourceCommandSha256(org.pangrova.protocol.v2.Checksum.newBuilder()
                        .setAlgorithm(org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(HexFormat.of().parseHex(scope.commandSha256()))))
                .setResourceGrant(StageResourceGrant.newBuilder().setGrantId(taskId + ":grant").setCpuSlots(1)
                        .setJvmHeapBytes(HEAP).setObjectStoreReadBytes(1024).setObjectStoreWriteBytes(1024).setSpillBytes(1024))
                .setDeadlineEpochMs(deadline).build();
        return PrepareCopySourceRequest.from(command, new CopySourceReadHttp.RedeemRequest("n".repeat(43), scope));
    }

    private static final class Fixture implements AutoCloseable {
        private final TaskManager manager = TaskManager.forHttpWiringOnly();
        private final org.pangrova.resource.memory.NodeMemoryPool node =
                new RootMemoryPool("root", 4 * HEAP).createNodePool("node", 4 * HEAP);
        private final TenantResourceTracker tracker = new TenantResourceTracker();
        private final TenantExecutionQuotaEnforcer quota = new TenantExecutionQuotaEnforcer(tracker);
        private final SourcePreparationControl control;

        private Fixture(CopyStageLeases.Stager stager) { this(stager, (nonce, scope, proof) -> PROOF); }
        private Fixture(CopyStageLeases.Stager stager, CopyStageLeases.ProofValidator validator) {
            manager.setQuotaEnforcer(quota);
            control = new SourcePreparationControl(manager, new CopyStageLeases(stager, validator, Clock.systemUTC(), null), node);
        }
        private void assertReleased() {
            assertThat(node.getUsedBytes()).isZero();
            assertThat(node.getDoubleReleaseCount()).isZero();
            assertThat(quota.getTenantActiveQueryCount("tenant")).isZero();
            assertThat(quota.getTenantMemoryUsage("tenant")).isZero();
        }
        @Override public void close() throws Exception {
            try { control.close(); } finally { manager.close(); }
        }
    }
}
