package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.indexing.api.metadata.CommitEventRequest;
import org.pangrova.persistence.indexing.api.metadata.CommitLogEntry;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcIndexCommitQueueTest {

    private DataSource dataSource;
    private JdbcIndexMetadataStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("commit_heads");
        store = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
    }

    @Test
    void acknowledgementAdvancesOnlyTheCurrentHeadAndSurvivesRestart() {
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        store.recordCommitEvent(request("table", "first", now.plusSeconds(10)));
        store.recordCommitEvent(request("table", "second", now));
        List<CommitLogEntry> batch = store.pollUnprocessedCommitEvents(2, 10L);
        assertThat(batch).extracting(CommitLogEntry::snapshotId)
                .containsExactly("first", "second");

        assertThatThrownBy(() -> store.markCommitEventProcessed(batch.get(1).eventId(), 10L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not the current table head");
        store.markCommitEventProcessed(batch.get(0).eventId(), 10L);
        JdbcIndexMetadataStore restarted = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        assertThat(restarted.pollUnprocessedCommitEvents(2, 11L))
                .extracting(CommitLogEntry::snapshotId).containsExactly("second");
        restarted.markCommitEventProcessed(batch.get(1).eventId(), 11L);
        assertThat(restarted.pollUnprocessedCommitEvents(2, 11L)).isEmpty();

        restarted.recordCommitEvent(request("table", "third", now.minusSeconds(10)));
        assertThat(restarted.pollUnprocessedCommitEvents(2, 11L))
                .extracting(CommitLogEntry::snapshotId).containsExactly("third");
    }

    @Test
    void newerLeaseFencesOldAcknowledgementWithoutSkippingHead() {
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        store.recordCommitEvent(request("table", "first", now));
        store.recordCommitEvent(request("table", "second", now.plusSeconds(1)));
        CommitLogEntry first = store.pollUnprocessedCommitEvents(2, 20L).get(0);
        assertThat(store.pollUnprocessedCommitEvents(2, 21L).get(0).eventId())
                .isEqualTo(first.eventId());

        assertThatThrownBy(() -> store.markCommitEventProcessed(first.eventId(), 20L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Stale index commit-event lease epoch");
        assertThat(store.pollUnprocessedCommitEvents(2, 21L).get(0).eventId())
                .isEqualTo(first.eventId());
        store.markCommitEventProcessed(first.eventId(), 21L);
        assertThat(store.pollUnprocessedCommitEvents(2, 21L))
                .extracting(CommitLogEntry::snapshotId).containsExactly("second");
    }

    @Test
    void failedInsertRollsBackHeadAllocation() throws SQLException {
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        var invalid = new CommitEventRequest(
                "tenant", "catalog", "schema", "table", "invalid",
                "not-json", "[]", "APPEND", now);
        assertThatThrownBy(() -> store.recordCommitEvent(invalid)).isInstanceOf(RuntimeException.class);
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT COUNT(*) FROM index_commit_table_heads")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getLong(1)).isZero();
        }

        store.recordCommitEvent(request("table", "valid", now));
        assertThat(store.pollUnprocessedCommitEvents(1, 1L))
                .extracting(CommitLogEntry::snapshotId).containsExactly("valid");
    }

    @Test
    void sequenceOverflowRollsBackInsertedEvent() throws SQLException {
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        store.recordCommitEvent(request("table", "first", now));
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE index_commit_table_heads "
                    + "SET next_sequence=9223372036854775807 WHERE table_name='table'");
        }
        assertThatThrownBy(() -> store.recordCommitEvent(request("table", "overflow", now)))
                .isInstanceOf(ArithmeticException.class);
        assertThat(store.pollUnprocessedCommitEvents(2, 1L))
                .extracting(CommitLogEntry::snapshotId).containsExactly("first");
    }

    @Test
    void hotTableDoesNotHideAnotherTablesHead() {
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        for (int i = 0; i < 100; i++) {
            store.recordCommitEvent(request("hot", "hot-" + i, now.plusSeconds(i)));
        }
        store.recordCommitEvent(request("healthy", "healthy-0", now.plusSeconds(200)));

        assertThat(store.pollUnprocessedCommitEvents(3, 1L))
                .extracting(CommitLogEntry::snapshotId)
                .containsExactly("hot-0", "healthy-0", "hot-1");
    }

    @Test
    void lockedHeadIsSkippedWithoutBlockingOtherTables() throws SQLException {
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        store.recordCommitEvent(request("alpha", "alpha-head", now));
        store.recordCommitEvent(request("beta", "beta-head", now));
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (var lock = blocker.prepareStatement(
                    "SELECT head_event_id FROM index_commit_table_heads "
                            + "WHERE table_name='alpha' FOR UPDATE");
                    var ignored = lock.executeQuery()) {
                assertThat(ignored.next()).isTrue();
                assertThat(store.pollUnprocessedCommitEvents(1, 1L))
                        .extracting(CommitLogEntry::snapshotId)
                        .containsExactly("beta-head");
            } finally {
                blocker.rollback();
            }
        }
        assertThat(store.pollUnprocessedCommitEvents(1, 1L))
                .extracting(CommitLogEntry::snapshotId)
                .containsExactly("alpha-head");
    }

    @Test
    void concurrentWritersAllocateOneDurableTableOrder() throws Exception {
        Instant now = Instant.parse("2026-09-05T01:02:03Z");
        CountDownLatch start = new CountDownLatch(1);
        var writers = Executors.newFixedThreadPool(2);
        try {
            var first = writers.submit(() -> insertRange(start, now, 0, 20));
            var second = writers.submit(() -> insertRange(start, now, 20, 40));
            start.countDown();
            first.get(60, TimeUnit.SECONDS);
            second.get(60, TimeUnit.SECONDS);
        } finally {
            writers.shutdownNow();
        }

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT table_sequence FROM index_commit_log "
                        + "ORDER BY table_sequence")) {
            for (long expected = 1; expected <= 40; expected++) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isEqualTo(expected);
            }
            assertThat(rows.next()).isFalse();
        }
        List<CommitLogEntry> entries = store.pollUnprocessedCommitEvents(40, 3L);
        assertThat(entries).hasSize(40);
        for (CommitLogEntry entry : entries) {
            store.markCommitEventProcessed(entry.eventId(), 3L);
        }
        assertThat(store.pollUnprocessedCommitEvents(1, 3L)).isEmpty();
    }

    private void insertRange(CountDownLatch start, Instant now, int from, int to) {
        try {
            start.await();
            for (int i = from; i < to; i++) {
                store.recordCommitEvent(request("concurrent", "snapshot-" + i,
                        now.plusSeconds(i)));
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Concurrent writer was interrupted", failure);
        }
    }

    private static CommitEventRequest request(String table, String snapshot, Instant createdAt) {
        return new CommitEventRequest(
                "tenant", "catalog", "schema", table, snapshot,
                "[]", "[]", "APPEND", createdAt);
    }
}
