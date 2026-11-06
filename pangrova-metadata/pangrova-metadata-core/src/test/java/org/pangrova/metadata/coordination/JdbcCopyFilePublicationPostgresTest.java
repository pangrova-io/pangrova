package org.pangrova.metadata.coordination;
import org.pangrova.persistence.query.postgresql.transaction.JdbcWriteCommitJournal;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.MetadataWriteReceiptFixtures;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.file.api.claim.CopyFileClaim;
import org.pangrova.persistence.file.api.claim.CopyFileClaimStore;
import org.pangrova.persistence.file.postgresql.claim.JdbcCopyFileClaimStore;
import org.pangrova.common.file.CopyFilePublication;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL publication ordering, immutable replay, and concurrent outcome tests. */
class JdbcCopyFilePublicationPostgresTest {

    private static final String TABLE_UUID = "00000000-0000-0000-0000-000000000001";
    private static final String PROFILE_UUID = "00000000-0000-0000-0000-000000000002";

    @Test
    void publicationMustPrecedeCommitAndSurvivesJournalPurge() throws Exception {
        DataSource source = SharedPostgresContainer.migratedDataSource("copy_publish_purge");
        WriteCommitEntry intent = admittedIntent(source, "copy-execution");
        JdbcWriteCommitJournal journal = new JdbcWriteCommitJournal(source);
        CopyFilePublication proof = publication(intent, 73L);

        assertThatThrownBy(() -> journal.markCommitted(intent.queryId()))
                .hasStackTraceContaining("COPY journal cannot commit before durable publication proof");
        journal.recordCopyFilePublication(intent, proof);
        assertThat(new JdbcWriteCommitJournal(source).findCopyFilePublication(intent)).contains(proof);
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE copy_file_publications SET snapshot_id=74"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.executeUpdate("DELETE FROM copy_file_publications"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
            assertThatThrownBy(() -> statement.execute("TRUNCATE copy_file_publications"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
        }
        journal.markCommitted(intent.queryId());
        journal.acknowledgeNoDeferredWork(intent.commitFingerprint());
        journal.purge(intent.queryId());
        assertThat(journal.getEntry(intent.queryId())).isEmpty();

        // Replaying after retirement must read the immutable row first. Its INSERT
        // trigger correctly rejects a new publication without a live journal intent.
        new JdbcWriteCommitJournal(source).recordCopyFilePublication(intent, proof);
        assertThat(journal.findCopyFilePublication(intent)).contains(proof);
        assertThatThrownBy(() -> journal.recordCopyFilePublication(intent, publication(intent, 74L)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("immutable proof");
    }

    @Test
    void directSqlCannotForgePublicationOrCommitWithoutAdmission() throws Exception {
        DataSource source = SharedPostgresContainer.migratedDataSource("copy_publish_guard");
        WriteCommitEntry intent = admittedIntent(source, "guarded-execution");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE write_commit_journal SET state='COMMITTED' WHERE query_id='guarded-execution'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("COPY journal cannot commit before durable publication proof");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "UPDATE write_commit_journal SET state='ABORTED' WHERE query_id='guarded-execution'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("COPY outcome cannot be declared aborted");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "DELETE FROM write_commit_journal WHERE query_id='guarded-execution'"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("COPY journal redo cannot be deleted");
            assertThatThrownBy(() -> statement.execute("TRUNCATE write_commit_journal"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Write commit journal cannot be truncated");
            assertThatThrownBy(() -> statement.executeUpdate(
                    "INSERT INTO copy_file_publications "
                    + "(claim_key,load_generation,claim_digest,execution_id,tenant_id,table_uuid,"
                    + "commit_fingerprint,snapshot_id) VALUES ('" + intent.copyFile().fileKey()
                    + "',0,'" + intent.copyFile().claimDigest() + "','wrong-execution','tenant-a','"
                    + TABLE_UUID + "','" + intent.commitFingerprint() + "',73)"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("COPY publication differs from its durable write intent");
        }
        assertThat(new JdbcWriteCommitJournal(source).findCopyFilePublication(intent)).isEmpty();
    }

    @Test
    void competingSnapshotProofsHaveOneDurableWinner() throws Exception {
        DataSource source = SharedPostgresContainer.migratedDataSource("copy_publish_race");
        WriteCommitEntry intent = admittedIntent(source, "racing-execution");
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var first = executor.submit(() -> recordAfter(source, intent, 81L, start));
            var second = executor.submit(() -> recordAfter(source, intent, 82L, start));
            start.countDown();
            boolean firstWon = first.get(10, TimeUnit.SECONDS);
            boolean secondWon = second.get(10, TimeUnit.SECONDS);
            assertThat(firstWon).isNotEqualTo(secondWon);
            assertThat(new JdbcWriteCommitJournal(source).findCopyFilePublication(intent).orElseThrow()
                    .snapshotId()).isEqualTo(firstWon ? 81L : 82L);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void copyIntentRequiresTheExactAdmittedFileClaim() throws Exception {
        DataSource source = SharedPostgresContainer.migratedDataSource("copy_intent_admission");
        CopyFileClaim claim = claim("admission-execution");
        WriteCommitEntry intent = copyIntent(claim);
        JdbcWriteCommitJournal journal = new JdbcWriteCommitJournal(source);

        assertThatThrownBy(() -> journal.recordIntent(intent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("durable file admission");
        assertThat(new JdbcCopyFileClaimStore(source).claim(claim))
                .isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);
        journal.recordIntent(intent);
        assertThat(journal.getEntry(intent.queryId()).orElseThrow().copyFile())
                .isEqualTo(claim.commitBinding());
    }

    private static boolean recordAfter(DataSource source, WriteCommitEntry intent,
            long snapshotId, CountDownLatch start) throws InterruptedException {
        if (!start.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Concurrent publication start timed out");
        }
        try {
            new JdbcWriteCommitJournal(source).recordCopyFilePublication(intent, publication(intent, snapshotId));
            return true;
        } catch (IllegalStateException conflict) {
            assertThat(conflict).hasMessageContaining("immutable proof");
            return false;
        }
    }

    private static WriteCommitEntry admittedIntent(DataSource source, String queryId) {
        CopyFileClaim claim = claim(queryId);
        assertThat(new JdbcCopyFileClaimStore(source).claim(claim))
                .isEqualTo(CopyFileClaimStore.ClaimResult.ACQUIRED);
        WriteCommitEntry intent = copyIntent(claim);
        new JdbcWriteCommitJournal(source).recordIntent(intent);
        return intent;
    }

    private static CopyFileClaim claim(String queryId) {
        return new CopyFileClaim("tenant-a", TABLE_UUID,
                "s3://bucket/data/file.parquet", "source-version", "a".repeat(64), 4L,
                "profile", PROFILE_UUID, 1L, 0L, queryId);
    }

    private static WriteCommitEntry copyIntent(CopyFileClaim claim) {
        String queryId = claim.executionId();
        var receipt = MetadataWriteReceiptFixtures.receipt();
        var fragments = List.of(new byte[]{1, 2, 3});
        byte[] output = new byte[]{42};
        String fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(
                queryId, "tenant-a", "iceberg", "analytics.events", "INSERT", 42L,
                output, "alice", receipt, claim.commitBinding()), fragments);
        WriteCommitEntry intent = new WriteCommitEntry(queryId, "iceberg", "analytics.events",
                WriteCommitEntry.State.PENDING, 1_000L, fragments, "INSERT", fingerprint,
                "tenant-a", "alice", receipt, 42L, output, fingerprint,
                "coordinator-a", "process-a", 7L, null, claim.commitBinding());
        return intent;
    }

    private static CopyFilePublication publication(WriteCommitEntry intent, long snapshotId) {
        return new CopyFilePublication(intent.copyFile(), intent.queryId(),
                intent.commitFingerprint(), snapshotId);
    }
}
