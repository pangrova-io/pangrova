package org.pangrova.metadata.coordination;

import org.pangrova.spi.coordination.JournalWriteException;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link JdbcWriteCommitJournal} using a PostgreSQL TestContainer.
 * Verifies the full intent → commit → purge lifecycle and idempotency guarantees.
 */
class JdbcWriteCommitJournalTest {

    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test")
            .withUsername("pangrova")
            .withPassword("pangrova_test");

    @BeforeAll
    static void startContainer() { PG.start(); }

    @AfterAll
    static void stopContainer() { PG.stop(); }

    private DataSource dataSource;
    private JdbcWriteCommitJournal journal;

    @BeforeEach
    void setUp() throws SQLException {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        dataSource = ds;
        // Drop and recreate so each test starts clean
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute("DROP TABLE IF EXISTS write_commit_journal");
        }
        journal = new JdbcWriteCommitJournal(dataSource);
    }

    @Test
    void recordIntentPersistsEntry() {
        WriteCommitEntry entry = new WriteCommitEntry(
                "q-1", "catalog-a", "schema.table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis());

        journal.recordIntent(entry);

        Optional<WriteCommitEntry> retrieved = journal.getEntry("q-1");
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().queryId()).isEqualTo("q-1");
        assertThat(retrieved.get().catalogName()).isEqualTo("catalog-a");
        assertThat(retrieved.get().tableName()).isEqualTo("schema.table");
        assertThat(retrieved.get().state()).isEqualTo(WriteCommitEntry.State.PENDING);
    }

    @Test
    void recordIntentPersistsReplayPayloadAcrossDatabaseRoundTrip() {
        // A-COMMIT-01: the durable entry must be a replayable redo record — the worker commit
        // fragments, write mode and idempotency key must survive a DB round-trip so a replacement
        // coordinator can reconstruct the commit after a crash rather than just observe a marker.
        java.util.List<byte[]> fragments = java.util.List.of(
                new byte[]{1, 2, 3}, new byte[]{4, 5, 6, 7});
        journal.recordIntent(new WriteCommitEntry(
                "q-replay", "iceberg", "analytics.events",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis(),
                fragments, "OVERWRITE", "idem-key-1", "coord-A", 7L));

        WriteCommitEntry reloaded = journal.getEntry("q-replay").orElseThrow();
        assertThat(reloaded.fragments()).hasSize(2);
        assertThat(reloaded.fragments().get(0)).isEqualTo(new byte[]{1, 2, 3});
        assertThat(reloaded.fragments().get(1)).isEqualTo(new byte[]{4, 5, 6, 7});
        assertThat(reloaded.writeMode()).isEqualTo("OVERWRITE");
        assertThat(reloaded.idempotencyKey()).isEqualTo("idem-key-1");
        assertThat(reloaded.coordinatorId()).isEqualTo("coord-A");
        assertThat(reloaded.term()).isEqualTo(7L);
        assertThat(reloaded.isReplayable()).isTrue();
    }

    @Test
    void markCommittedUpdatesState() {
        journal.recordIntent(new WriteCommitEntry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        journal.markCommitted("q-1");

        Optional<WriteCommitEntry> entry = journal.getEntry("q-1");
        assertThat(entry).isPresent();
        assertThat(entry.get().state()).isEqualTo(WriteCommitEntry.State.COMMITTED);
    }

    @Test
    void isCommittedReturnsTrueAfterMarkCommitted() {
        journal.recordIntent(new WriteCommitEntry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        assertThat(journal.isCommitted("q-1")).isFalse();

        journal.markCommitted("q-1");

        assertThat(journal.isCommitted("q-1")).isTrue();
    }

    @Test
    void isCommittedReturnsFalseForNonExistentQuery() {
        assertThat(journal.isCommitted("nonexistent")).isFalse();
    }

    @Test
    void purgeRemovesEntry() {
        journal.recordIntent(new WriteCommitEntry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        journal.purge("q-1");

        assertThat(journal.getEntry("q-1")).isEmpty();
        assertThat(journal.isCommitted("q-1")).isFalse();
    }

    @Test
    void replayPendingEntriesReturnsOnlyPending() {
        journal.recordIntent(new WriteCommitEntry(
                "q-1", "catalog-a", "table1",
                WriteCommitEntry.State.PENDING, 1000L));
        journal.recordIntent(new WriteCommitEntry(
                "q-2", "catalog-a", "table2",
                WriteCommitEntry.State.PENDING, 2000L));
        journal.recordIntent(new WriteCommitEntry(
                "q-3", "catalog-b", "table3",
                WriteCommitEntry.State.PENDING, 3000L));

        // Mark one as committed
        journal.markCommitted("q-2");

        List<WriteCommitEntry> pending = journal.replayPendingEntries();
        assertThat(pending).hasSize(2);
        assertThat(pending.get(0).queryId()).isEqualTo("q-1");
        assertThat(pending.get(1).queryId()).isEqualTo("q-3");
    }

    @Test
    void replayPendingEntriesReturnsEmptyWhenAllCommitted() {
        journal.recordIntent(new WriteCommitEntry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, 1000L));
        journal.markCommitted("q-1");

        assertThat(journal.replayPendingEntries()).isEmpty();
    }

    @Test
    void replayPendingEntriesOrderedByTimestamp() {
        journal.recordIntent(new WriteCommitEntry(
                "q-late", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, 3000L));
        journal.recordIntent(new WriteCommitEntry(
                "q-early", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, 1000L));

        List<WriteCommitEntry> pending = journal.replayPendingEntries();
        assertThat(pending).hasSize(2);
        assertThat(pending.get(0).queryId()).isEqualTo("q-early");
        assertThat(pending.get(1).queryId()).isEqualTo("q-late");
    }

    @Test
    void duplicateIntentThrows() {
        journal.recordIntent(new WriteCommitEntry(
                "q-1", "catalog-a", "table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis()));

        assertThatThrownBy(() -> journal.recordIntent(new WriteCommitEntry(
                "q-1", "catalog-b", "other-table",
                WriteCommitEntry.State.PENDING, System.currentTimeMillis())))
                .isInstanceOf(JournalWriteException.class);
    }

    @Test
    void markCommittedOnNonExistentEntryIsNoOp() {
        // Should not throw, just logs a warning
        journal.markCommitted("nonexistent");

        assertThat(journal.isCommitted("nonexistent")).isFalse();
    }

    @Test
    void purgeNonExistentEntryIsNoOp() {
        // Should not throw
        journal.purge("nonexistent");
    }

    @Test
    void tableAutoCreatedOnConstruction() throws SQLException {
        // Verify the table was created by JdbcWriteCommitJournal constructor
        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, null,
                     "write_commit_journal", null)) {
            assertThat(rs.next()).isTrue();
        }
    }

    @Test
    void getEntryReturnsEmptyForNonExistent() {
        assertThat(journal.getEntry("does-not-exist")).isEmpty();
    }
}
