package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.enums.IcebergCommitOutcome;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.pangrova.common.concurrency.ThreadSleeper;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies whether an Iceberg commit actually succeeded after a timeout.
 *
 * <p>When a commit future times out, the commit may have succeeded on the catalog
 * side (the timeout only means we didn't get confirmation in time). This verifier
 * refreshes the table metadata and checks for proof of our commit, so that a
 * timed-out commit is never blindly retried (data duplication) or abandoned
 * (data loss).
 *
 * <p>Proof of success is strictly positive: the snapshot carrying our
 * fingerprint marker must be present and in the current snapshot ancestry.
 * Without a durable commit context there is no way to attribute a new
 * snapshot to our commit — a concurrent writer's snapshot descends from the
 * same base — so an advanced table is always {@link IcebergCommitOutcome#UNKNOWN}.
 *
 * <p>An unchanged snapshot is not proof of failure: a cancelled task may still
 * be executing in a remote catalog. Absence of a marker always remains UNKNOWN.
 */
final class CommitStatusVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(CommitStatusVerifier.class);

    /**
     * Grace period after initial timeout before checking again, allowing in-flight
     * commits that are almost done to complete.
     */
    static final long VERIFICATION_GRACE_MS = 5_000L;

    /**
     * Maximum number of verification attempts (initial check + retries after grace).
     */
    static final int MAX_VERIFICATION_ATTEMPTS = 2;

    /**
     * Result of commit status verification, including the outcome and diagnostic context.
     */
    record VerificationResult(IcebergCommitOutcome outcome, String detail) {}

    private CommitStatusVerifier() {
    }

    /**
     * Verifies a commit that carried no durable commit context. Without a marker
     * an advanced snapshot cannot be attributed to this commit, so advancement is
     * reported as {@link IcebergCommitOutcome#UNKNOWN}. An unchanged snapshot also remains
     * unknown because a remote commit may still publish after verification.
     */
    static VerificationResult verify(Table table, long preCommitSnapshotId, String tableId) {
        return verify(table, preCommitSnapshotId, null, tableId);
    }

    /**
     * Verifies whether a timed-out commit succeeded.
     *
     * @param table               the Iceberg table (will be refreshed)
     * @param preCommitSnapshotId the snapshot ID recorded before the commit was submitted,
     *                            or -1 if the table had no snapshots before the commit
     * @param context             durable commit context, or null when the commit has none
     * @param tableId             table identifier for logging
     * @return verification result indicating whether the commit succeeded or is unknown
     */
    static VerificationResult verify(Table table, long preCommitSnapshotId,
            DistributedWriteCommitContext context, String tableId) {
        boolean advancedWithoutProof = false;
        for (int attempt = 0; attempt < MAX_VERIFICATION_ATTEMPTS; attempt++) {
            if (attempt > 0) {
                sleep(VERIFICATION_GRACE_MS, tableId);
            }
            try {
                table.refresh();
                advancedWithoutProof |= advancedSince(table, preCommitSnapshotId);
                if (context != null) {
                    if (IcebergDistributedCommitMarker.isPublished(table, context)) {
                        LOG.info("event=commit_timeout_verified_success table={} "
                                        + "proof=commit_marker attempt={}",
                                tableId, attempt + 1);
                        return new VerificationResult(IcebergCommitOutcome.SUCCEEDED,
                                "Commit fingerprint marker is present in the current snapshot ancestry");
                    }
                }
                logPendingAttempt(tableId, preCommitSnapshotId, table.currentSnapshot(), attempt);
            } catch (Exception e) {
                LOG.warn("event=commit_timeout_verification_error table={} attempt={} error={}",
                        tableId, attempt + 1, e.getMessage());
                return new VerificationResult(IcebergCommitOutcome.UNKNOWN,
                        "Verification failed: " + e.getMessage());
            }
        }

        if (advancedWithoutProof) {
            LOG.warn("event=commit_timeout_unprovable table={} preCommitSnapshot={} "
                            + "reason=snapshot_advanced_without_commit_marker",
                    tableId, preCommitSnapshotId);
            return new VerificationResult(IcebergCommitOutcome.UNKNOWN,
                    "Snapshot advanced without provable Pangrova commit evidence; "
                            + "another writer may have committed");
        }

        LOG.warn("event=commit_timeout_unprovable table={} "
                        + "preCommitSnapshot={} reason=snapshot_unchanged_after_grace",
                tableId, preCommitSnapshotId);
        return new VerificationResult(IcebergCommitOutcome.UNKNOWN,
                "Snapshot unchanged after %d verification attempts (grace=%dms); remote commit may still publish"
                        .formatted(MAX_VERIFICATION_ATTEMPTS, VERIFICATION_GRACE_MS));
    }

    private static boolean advancedSince(Table table, long preCommitSnapshotId) {
        Snapshot current = table.currentSnapshot();
        return current != null && current.snapshotId() != preCommitSnapshotId;
    }

    private static void logPendingAttempt(String tableId, long preCommitSnapshotId,
                                           Snapshot currentSnapshot, int attempt) {
        if (attempt < MAX_VERIFICATION_ATTEMPTS - 1) {
            LOG.debug("event=commit_timeout_verification_pending table={} "
                            + "preCommitSnapshot={} currentSnapshot={} attempt={}",
                    tableId, preCommitSnapshotId,
                    currentSnapshot != null ? currentSnapshot.snapshotId() : "none",
                    attempt + 1);
        }
    }

    private static void sleep(long millis, String tableId) {
        boolean interruptedBefore = Thread.currentThread().isInterrupted();
        ThreadSleeper.sleepQuietly(millis);
        if (!interruptedBefore && Thread.currentThread().isInterrupted()) {
            LOG.warn("event=commit_verification_interrupted table={}", tableId);
        }
    }
}
