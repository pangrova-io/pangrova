package org.pangrova.connector.lance;

import org.pangrova.persistence.query.api.enums.WriteCommitState;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Actual PG late head locks and official SDK objects; the fixture's original 60-second lease is unchanged. */
class ManagedLancePublicationFenceIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test
    void finalAcceptanceClockRejectsLockWaitPastActualLeaseAndRollsBackAllPointers() throws Exception {
        acceptanceAfterHeadLock(true);
    }

    @Test
    void finalAcceptanceCommitsAllPointersWhenActualHeadLockEndsBeforeLease() throws Exception {
        acceptanceAfterHeadLock(false);
    }

    private void acceptanceAfterHeadLock(boolean expire) throws Exception {
        var fixture = new LanceManagedSourceFixture("final-fence-" + UUID.randomUUID(), directory);
        var prepared = LancePgSdkTestInputs.write(fixture, directory, directory.resolve("fenced.lance").toString(),
                "fenced-write", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 7, 9);
        assertThat(prepared.pages()).hasSize(1);
        var allocation = prepared.allocation(); var result = prepared.pages().get(0);
        var publication = fixture.registerObjects(allocation.executionId(), result);
        var original = fixture.journal().getEntry(allocation.executionId()).orElseThrow();
        var executor = ManagedExecutorFactory.newBoundedThreadPool("actual-source-final-fence", 1, 1);
        try (var blocker = fixture.dataSource().getConnection(); var observer = fixture.dataSource().getConnection()) {
            blocker.setAutoCommit(false);
            int blockerPid = backendPid(blocker);
            lockHead(blocker, fixture, allocation);
            var accepted = executor.submit(() -> fixture.acceptRegistered(allocation.executionId(), publication));
            var observedStart = new AtomicReference<Instant>();
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).until(() -> {
                var started = blockedTransactionStart(observer, blockerPid);
                observedStart.set(started);
                return started != null;
            });
            assertThat(observedStart.get()).isAfterOrEqualTo(allocation.anchor().acquiredAt())
                    .isBefore(allocation.anchor().expiresAt());
            assertThat(dbNow(observer)).isBefore(allocation.anchor().expiresAt());
            assertThat(accepted.isDone()).isFalse();
            if (expire) {
                // PostgreSQL itself waits until its actual lease deadline; no client/fake clock or expiry mutation.
                waitUntilDbClockPasses(observer, allocation.anchor().expiresAt());
                assertThat(dbNow(observer)).isAfter(allocation.anchor().expiresAt());
                assertThat(blockedTransactionStart(observer, blockerPid)).isEqualTo(observedStart.get());
            }
            blocker.commit();
            if (expire) {
                assertThatThrownBy(() -> accepted.get(15, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(IllegalStateException.class)
                        .hasRootCauseMessage("Execution publication owner is absent, replaced or expired");
                assertThat(fixture.store().load(fixture.catalog().getCatalogName(), LanceManagedSourceFixture.TENANT,
                        allocation.output().table().table()).version()).isZero();
                var current = fixture.journal().getEntry(original.queryId()).orElseThrow();
                assertThat(current.state()).isEqualTo(WriteCommitState.PENDING);
                assertThat(current.publication()).isNull();
                assertThat(current.commitFingerprint()).isEqualTo(original.commitFingerprint());
                assertThat(current.writeOutputBinding()).isEqualTo(original.writeOutputBinding());
                assertThat(current.fragments().get(0)).isEqualTo(original.fragments().get(0));
                assertPublicationState(observer, original.commitFingerprint(), "PREPARED");
            } else {
                var reference = accepted.get(15, TimeUnit.SECONDS);
                assertThat(reference.getVersion()).isEqualTo(1);
                assertThat(fixture.reference(reference.getDatasetUri(), 0)).isEqualTo(reference);
                var current = fixture.journal().getEntry(original.queryId()).orElseThrow();
                assertThat(current.state()).isEqualTo(WriteCommitState.COMMITTED);
                assertThat(current.publication().publishedVersion()).isEqualTo(reference.getVersion());
                assertThat(current.publication().manifestSha256()).isEqualTo(reference.getManifestSha256().getDigest().toByteArray());
                assertPublicationState(observer, original.commitFingerprint(), "ACCEPTED");
                try (var allocator = new RootAllocator()) {
                    assertThat(prepared.sdk().read(reference, allocator)).containsExactly(List.of(7), List.of(9));
                }
            }
            for (var object : result.getObjectPage().getObjectsList()) {
                Path path = Path.of(allocation.output().allocation().datasetUri()).resolve(object.getRelativePath());
                assertThat(Files.size(path)).isEqualTo(object.getSizeBytes());
            }
        } finally {
            executor.shutdownNow(); assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void lockHead(Connection connection, LanceManagedSourceFixture fixture,
            LanceManagedSourceFixture.Allocation allocation) throws Exception {
        try (var statement = connection.prepareStatement("SELECT visible_version FROM managed_source_heads "
                + "WHERE catalog_name=? AND tenant_id=? AND table_id=? FOR UPDATE")) {
            statement.setString(1, fixture.catalog().getCatalogName()); statement.setString(2, LanceManagedSourceFixture.TENANT);
            statement.setString(3, allocation.output().table().tableId());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue(); assertThat(result.getLong(1)).isZero(); assertThat(result.next()).isFalse();
            }
        }
    }
    private static int backendPid(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(result.next()).isTrue(); return result.getInt(1);
        }
    }
    private static Instant blockedTransactionStart(Connection connection, int blockerPid) throws Exception {
        try (var statement = connection.prepareStatement("SELECT xact_start FROM pg_stat_activity "
                + "WHERE ? = ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'")) {
            statement.setInt(1, blockerPid);
            try (var result = statement.executeQuery()) {
                if (!result.next()) return null;
                Instant start = result.getTimestamp(1).toInstant(); assertThat(result.next()).isFalse(); return start;
            }
        }
    }
    private static Instant dbNow(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT clock_timestamp()")) {
            assertThat(result.next()).isTrue(); return result.getTimestamp(1).toInstant();
        }
    }
    private static void waitUntilDbClockPasses(Connection connection, Instant deadline) throws Exception {
        try (var statement = connection.prepareStatement("SELECT pg_sleep_until(?)")) {
            statement.setQueryTimeout(70); statement.setObject(1, java.time.OffsetDateTime.ofInstant(deadline.plusMillis(20), java.time.ZoneOffset.UTC));
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); }
        }
    }
    private static void assertPublicationState(Connection connection, String fingerprint, String expected) throws Exception {
        try (var statement = connection.prepareStatement("SELECT state, registered_count, object_count "
                + "FROM managed_source_publications WHERE fingerprint=?")) {
            statement.setString(1, fingerprint);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue(); assertThat(result.getString(1)).isEqualTo(expected);
                assertThat(result.getLong(2)).isPositive().isEqualTo(result.getLong(3)); assertThat(result.next()).isFalse();
            }
        }
    }
}
