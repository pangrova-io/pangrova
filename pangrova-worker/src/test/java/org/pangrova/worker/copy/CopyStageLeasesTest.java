package org.pangrova.worker.copy;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.copy.preparation.PrepareCopySourceRequest;
import org.pangrova.protocol.rest.copy.preparation.PrepareCopySourceResponse;
import org.pangrova.protocol.v2.SourcePreparationRequest;
import org.pangrova.protocol.v2.Checksum;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopyStageLeasesTest {
    private static final long DEADLINE = 4_102_444_800_000L;
    private static final CopySourceReadHttp.StagedSource PROOF =
            new CopySourceReadHttp.StagedSource("version-1", "s3://managed/copy/object",
                    "a".repeat(64), 5, "b".repeat(64));

    @Test
    void concurrentRetryReturnsSameExactProofAndRejectsChangedAttempt() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var clock = new MutableClock(1_000);
        var pool = Executors.newFixedThreadPool(2);
        try (var leases = new CopyStageLeases((nonce, scope, budget) -> {
            calls.incrementAndGet();
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("timed out");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            return PROOF;
        }, (nonce, scope, proof) -> proof, clock, null)) {
            var task = task(DEADLINE, "n".repeat(43));
            var first = pool.submit(() -> leases.stage(task));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var retry = pool.submit(() -> leases.stage(task));
            release.countDown();
            assertThat(proof(first.get(5, TimeUnit.SECONDS))).isEqualTo(PROOF);
            assertThat(proof(retry.get(5, TimeUnit.SECONDS))).isEqualTo(PROOF);
            assertThat(calls.get()).isEqualTo(1);
            assertThatThrownBy(() -> leases.stage(task(DEADLINE + 1, "m".repeat(43))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("identity conflict");
            clock.set(DEADLINE + 1);
            assertThatThrownBy(() -> leases.stage(task))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expired");
            assertThat(calls.get()).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void closeWaitsForInFlightStageAndClosesOwnedResource() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var closedResource = new AtomicBoolean();
        var leases = new CopyStageLeases((nonce, scope, budget) -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("timed out");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            assertThat(closedResource).isFalse();
            return PROOF;
        }, (nonce, scope, proof) -> proof,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
                () -> closedResource.set(true));
        var pool = Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> leases.stage(task(DEADLINE, "n".repeat(43))));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var closing = pool.submit(() -> {
                leases.close();
                return true;
            });
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThatThrownBy(() -> leases.admit(task(DEADLINE, "n".repeat(43))))
                            .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed"));
            assertThat(closedResource).isFalse();
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS).getError().getErrorClass())
                    .isEqualTo(org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_CANCELLED);
            assertThat(closing.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(closedResource).isTrue();
            assertThatThrownBy(() -> leases.stage(task(DEADLINE, "n".repeat(43))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void failedStageRetainsEnteredUsageAndCannotReuseItsIssuedAllowance() throws Exception {
        var calls = new AtomicInteger();
        try (var leases = new CopyStageLeases((nonce, scope, budget) -> {
            budget.reserveRead(5);
            calls.incrementAndGet();
            throw new IOException("source unavailable");
        }, (nonce, scope, proof) -> proof,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), null)) {
            var task = task(DEADLINE, "n".repeat(43));
            var failed = leases.stage(task);
            assertThat(failed.hasError()).isTrue();
            assertThat(failed.getIoJoined()).isTrue();
            assertThat(failed.getUsage().getObjectStoreReadBytes()).isEqualTo(5);
            assertThat(new PrepareCopySourceResponse(java.util.Base64.getEncoder()
                    .encodeToString(failed.toByteArray())).resultFor(task)).isEqualTo(failed);
            assertThat(leases.stage(task)).isEqualTo(failed);
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void cachedProofRequiresFreshCoordinatorAuthorizationOnEveryRetry() throws Exception {
        var calls = new AtomicInteger();
        var authorized = new AtomicBoolean(true);
        try (var leases = new CopyStageLeases((nonce, scope, budget) -> {
            calls.incrementAndGet();
            return PROOF;
        }, (nonce, scope, proof) -> {
            if (!authorized.get()) {
                throw new org.pangrova.security.api.exception.AccessDeniedException(
                        "COPY permission revoked");
            }
            return proof;
        }, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), null)) {
            var task = task(DEADLINE, "n".repeat(43));
            assertThat(proof(leases.stage(task))).isEqualTo(PROOF);
            authorized.set(false);
            assertThatThrownBy(() -> leases.stage(task))
                    .isInstanceOf(org.pangrova.security.api.exception.AccessDeniedException.class);
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void expiredTombstoneRejectsChangedDeadlineThenReleasesCapacityAfterGrace()
            throws Exception {
        var clock = new MutableClock(1_000);
        var calls = new AtomicInteger();
        try (var leases = new CopyStageLeases((nonce, scope, budget) -> {
            calls.incrementAndGet();
            return PROOF;
        }, (nonce, scope, proof) -> proof, clock, null, 1)) {
            var first = task(2_000, "n".repeat(43), "task-42");
            assertThat(proof(leases.stage(first))).isEqualTo(PROOF);
            assertThatThrownBy(() -> leases.stage(task(3_000, "m".repeat(43), "task-42")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("identity conflict");
            assertThatThrownBy(() -> leases.stage(task(3_000, "x".repeat(43), "task-43")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("capacity");
            clock.set(2_000 + 10 * 60 * 1000L + 1);
            assertThatThrownBy(() -> leases.stage(first))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expired");
            assertThat(proof(leases.stage(task(DEADLINE, "x".repeat(43), "task-43"))))
                    .isEqualTo(PROOF);
            assertThat(calls.get()).isEqualTo(2);
        }
    }

    private static PrepareCopySourceRequest task(long deadline, String nonce) {
        return task(deadline, nonce, "task-42");
    }

    private static PrepareCopySourceRequest task(long deadline, String nonce, String taskId) {
        var scope = new CopySourceReadHttp.Scope("tenant", "test-user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1,
                "s3://incoming/file.parquet", "s3://managed/copy", "a".repeat(64),
                "query-7", taskId, "coordinator", "test", 1, 1, deadline);
        var canonical = SourcePreparationRequest.newBuilder().setProtocol(CanonicalProtocolIdentity.current())
                .setAttempt(org.pangrova.protocol.v2.TaskAttemptId.newBuilder().setTaskId(taskId)
                        .setQueryOrJobId("query-7").setPlanEpoch(1).setFencingToken(1L << 32 | 1L)
                        .setControlEpoch(1).setExecutionOwner(org.pangrova.protocol.v2.ExecutionOwnerIdentity.newBuilder()
                                .setOwnerId("coordinator").setOwnerIncarnation("test").setFencingToken(1)))
                .setSecurity(org.pangrova.protocol.v2.SecurityContext.newBuilder()
                        .setTenantId("tenant").setPrincipal("test-user"))
                .setSourceCommandSha256(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(HexFormat.of().parseHex(scope.commandSha256()))))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder().setGrantId(taskId + ":grant")
                        .setCpuSlots(1).setJvmHeapBytes(1024 * 1024).setObjectStoreReadBytes(1024)
                        .setObjectStoreWriteBytes(1024).setSpillBytes(1024))
                .setDeadlineEpochMs(deadline).build();
        return PrepareCopySourceRequest.from(canonical, new CopySourceReadHttp.RedeemRequest(nonce, scope));
    }

    private static CopySourceReadHttp.StagedSource proof(org.pangrova.protocol.v2.SourcePreparationResult result) {
        assertThat(result.hasError()).isFalse();
        assertThat(result.getIoJoined()).isTrue();
        var source = result.getSource();
        return new CopySourceReadHttp.StagedSource(source.getSourceVersionId(), source.getManagedObjectUri(),
                source.getContentSha256(), source.getContentSize(), source.getTenantScope());
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong time;

        private MutableClock(long time) { this.time = new AtomicLong(time); }
        private void set(long value) { time.set(value); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(time.get()); }
        @Override public long millis() { return time.get(); }
    }
}
