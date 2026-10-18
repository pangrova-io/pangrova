package org.pangrova.worker.copy;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.testing.nativefixture.CanonicalTaskRequestTestFixture;

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
                    "a".repeat(64), 5, "tenant-scope");

    @Test
    void concurrentRetryReturnsSameExactProofAndRejectsChangedAttempt() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var clock = new MutableClock(1_000);
        var pool = Executors.newFixedThreadPool(2);
        try (var leases = new CopyStageLeases((nonce, scope) -> {
            calls.incrementAndGet();
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("timed out");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            return PROOF;
        }, (nonce, scope, proof) -> {}, clock, null)) {
            var task = task(DEADLINE, "n".repeat(43));
            var first = pool.submit(() -> leases.stage(task));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var retry = pool.submit(() -> leases.stage(task));
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(PROOF);
            assertThat(retry.get(5, TimeUnit.SECONDS)).isEqualTo(PROOF);
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
        var leases = new CopyStageLeases((nonce, scope) -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("timed out");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            assertThat(closedResource).isFalse();
            return PROOF;
        }, (nonce, scope, proof) -> {},
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
            assertThat(closedResource).isFalse();
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(PROOF);
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
    void failedStageCanRetrySameTicketWithoutPublishingProof() throws Exception {
        var calls = new AtomicInteger();
        try (var leases = new CopyStageLeases((nonce, scope) -> {
            if (calls.incrementAndGet() == 1) throw new IOException("source unavailable");
            return PROOF;
        }, (nonce, scope, proof) -> {},
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), null)) {
            var task = task(DEADLINE, "n".repeat(43));
            assertThatThrownBy(() -> leases.stage(task)).isInstanceOf(IOException.class);
            assertThat(leases.stage(task)).isEqualTo(PROOF);
            assertThat(calls.get()).isEqualTo(2);
        }
    }

    @Test
    void cachedProofRequiresFreshCoordinatorAuthorizationOnEveryRetry() throws Exception {
        var calls = new AtomicInteger();
        var authorized = new AtomicBoolean(true);
        try (var leases = new CopyStageLeases((nonce, scope) -> {
            calls.incrementAndGet();
            return PROOF;
        }, (nonce, scope, proof) -> {
            if (!authorized.get()) {
                throw new org.pangrova.spi.security.AccessDeniedException(
                        "COPY permission revoked");
            }
        }, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), null)) {
            var task = task(DEADLINE, "n".repeat(43));
            assertThat(leases.stage(task)).isEqualTo(PROOF);
            authorized.set(false);
            assertThatThrownBy(() -> leases.stage(task))
                    .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class);
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void expiredTombstoneRejectsChangedDeadlineThenReleasesCapacityAfterGrace()
            throws Exception {
        var clock = new MutableClock(1_000);
        var calls = new AtomicInteger();
        try (var leases = new CopyStageLeases((nonce, scope) -> {
            calls.incrementAndGet();
            return PROOF;
        }, (nonce, scope, proof) -> {}, clock, null, 1)) {
            var first = task(2_000, "n".repeat(43), "task-42");
            assertThat(leases.stage(first)).isEqualTo(PROOF);
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
            assertThat(leases.stage(task(DEADLINE, "x".repeat(43), "task-43")))
                    .isEqualTo(PROOF);
            assertThat(calls.get()).isEqualTo(2);
        }
    }

    private static SubmitTaskRequest task(long deadline, String nonce) {
        return task(deadline, nonce, "task-42");
    }

    private static SubmitTaskRequest task(long deadline, String nonce, String taskId) {
        var scope = new CopySourceReadHttp.Scope("tenant", "test-user", "incoming",
                "f92e704f-2443-4d3a-a496-cd6507dd97db", 1,
                "s3://incoming/file.parquet", "s3://managed/copy", "a".repeat(64),
                "query-7", taskId, "coordinator", "test", 1, 1, deadline);
        var canonical = CanonicalTaskRequestTestFixture.builder(taskId, "query-7", 0, 0,
                        TaskKind.TASK_KIND_WRITE, new byte[] {1})
                .setCopySourceStagingCommandSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(
                                HexFormat.of().parseHex(scope.commandSha256()))))
                .setResourceGrant(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                        .setGrantId(taskId + ":grant").setCpuSlots(1)
                        .setNativeMemoryBytes(64L * 1024 * 1024))
                .setDeadlineEpochMs(deadline)
                .build();
        return SubmitTaskRequest.from(canonical, new CopySourceReadHttp.RedeemRequest(
                nonce, scope));
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
