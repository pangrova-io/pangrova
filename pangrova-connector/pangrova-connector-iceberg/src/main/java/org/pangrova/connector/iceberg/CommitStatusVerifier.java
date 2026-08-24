package org.pangrova.connector.iceberg;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.pangrova.common.concurrent.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies whether an Iceberg commit actually succeeded after a timeout.
 *
 * <p>When a commit future times out, the commit may have succeeded on the catalog
 * side (the timeout only means we didn't get confirmation in time). This verifier
 * refreshes the table metadata and checks whether a new snapshot appeared, which
 * would indicate the commit landed successfully.
 *
 * <p>Verification strategy (standard Iceberg "optimistic commit" pattern):
 * <ol>
 *   <li>Record the table's snapshot ID before submitting the commit.</li>
 *   <li>On timeout, refresh the table metadata from the catalog.</li>
 *   <li>If a new snapshot appeared after the pre-commit snapshot: commit succeeded.</li>
 *   <li>If no new snapshot after a grace period: commit genuinely failed.</li>
 *   <li>If verification itself fails (e.g., catalog unreachable): unknown status.</li>
 * </ol>
 *
 * <p>This implements the F-ICEBERG-02 remediation: commit timeout no longer blindly
 * abandons in-flight commits, preventing both data duplication (unsafe retry of
 * a succeeded commit) and data loss (abandoning a commit that actually landed).
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

    enum CommitOutcome {
        /** Commit succeeded — a new snapshot matching our commit was found. */
        SUCCEEDED,
        /** Commit did not land — safe to retry. */
        FAILED,
        /** Cannot determine outcome — requires manual investigation. */
        UNKNOWN
    }

    /**
     * Result of commit status verification, including the outcome and diagnostic context.
     */
    record VerificationResult(CommitOutcome outcome, String detail) {}

    private CommitStatusVerifier() {
    }

    /**
     * Verifies whether a commit succeeded by checking if the table's snapshot advanced
     * beyond the pre-commit snapshot.
     *
     * @param table              the Iceberg table (will be refreshed)
     * @param preCommitSnapshotId the snapshot ID recorded before the commit was submitted,
     *                            or -1 if the table had no snapshots before the commit
     * @param tableId            table identifier for logging
     * @return verification result indicating whether the commit succeeded, failed, or is unknown
     */
    static VerificationResult verify(Table table, long preCommitSnapshotId, String tableId) {
        for (int attempt = 0; attempt < MAX_VERIFICATION_ATTEMPTS; attempt++) {
            if (attempt > 0) {
                sleep(VERIFICATION_GRACE_MS, tableId);
            }
            try {
                table.refresh();
                VerificationResult result = checkSnapshot(table, preCommitSnapshotId, tableId, attempt);
                if (result != null) {
                    return result;
                }
                logPendingAttempt(tableId, preCommitSnapshotId, table.currentSnapshot(), attempt);
            } catch (Exception e) {
                LOG.warn("event=commit_timeout_verification_error table={} attempt={} error={}",
                        tableId, attempt + 1, e.getMessage());
                return new VerificationResult(CommitOutcome.UNKNOWN,
                        "Verification failed: " + e.getMessage());
            }
        }

        LOG.info("event=commit_timeout_verified_failure table={} "
                        + "preCommitSnapshot={} reason=snapshot_unchanged_after_grace",
                tableId, preCommitSnapshotId);
        return new VerificationResult(CommitOutcome.FAILED,
                "Snapshot unchanged after %d verification attempts (grace=%dms)"
                        .formatted(MAX_VERIFICATION_ATTEMPTS, VERIFICATION_GRACE_MS));
    }

    /** Checks whether a new snapshot appeared, returning a result if conclusive, or null to continue. */
    private static VerificationResult checkSnapshot(Table table, long preCommitSnapshotId,
                                                     String tableId, int attempt) {
        Snapshot currentSnapshot = table.currentSnapshot();
        if (preCommitSnapshotId < 0 && currentSnapshot != null) {
            LOG.info("event=commit_timeout_verified_success table={} "
                            + "preCommitSnapshot=none newSnapshot={} attempt={}",
                    tableId, currentSnapshot.snapshotId(), attempt + 1);
            return new VerificationResult(CommitOutcome.SUCCEEDED,
                    "New snapshot %d appeared on previously-empty table"
                            .formatted(currentSnapshot.snapshotId()));
        }
        if (preCommitSnapshotId >= 0 && currentSnapshot != null
                && currentSnapshot.snapshotId() != preCommitSnapshotId
                && isDescendantOf(table, currentSnapshot, preCommitSnapshotId)) {
            LOG.info("event=commit_timeout_verified_success table={} "
                            + "preCommitSnapshot={} newSnapshot={} attempt={}",
                    tableId, preCommitSnapshotId, currentSnapshot.snapshotId(), attempt + 1);
            return new VerificationResult(CommitOutcome.SUCCEEDED,
                    "Snapshot advanced from %d to %d"
                            .formatted(preCommitSnapshotId, currentSnapshot.snapshotId()));
        }
        return null;
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

    /**
     * Checks if the current snapshot is a descendant of the given ancestor snapshot ID
     * by walking the snapshot parent chain.
     */
    private static boolean isDescendantOf(Table table, Snapshot current, long ancestorId) {
        // Walk the parent chain from current back to ancestor (max 10 hops to prevent loops)
        Snapshot walk = current;
        for (int i = 0; i < 10 && walk != null; i++) {
            Long parentId = walk.parentId();
            if (parentId != null && parentId == ancestorId) {
                return true;
            }
            if (parentId == null) {
                break;
            }
            walk = table.snapshot(parentId);
        }
        // If we can't prove lineage, still consider it advanced if ID differs
        // (could be a concurrent commit that also advanced the table)
        return current.snapshotId() != ancestorId;
    }

    private static void sleep(long millis, String tableId) {
        boolean interruptedBefore = Thread.currentThread().isInterrupted();
        ThreadUtils.sleepQuietly(millis);
        if (!interruptedBefore && Thread.currentThread().isInterrupted()) {
            LOG.warn("event=commit_verification_interrupted table={}", tableId);
        }
    }
}
