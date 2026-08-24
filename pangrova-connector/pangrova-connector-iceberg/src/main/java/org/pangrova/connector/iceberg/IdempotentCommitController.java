package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.ConnectorSession;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Controls idempotent commit behavior for Iceberg write operations.
 *
 * <p>Implements F-ICEBERG-02: prevents duplicate commits when a coordinator
 * crashes during or after a commit and retries the operation. Uses a commit
 * token ({@code queryId:attemptId}) stored in Iceberg snapshot summary
 * properties to detect and skip already-committed writes.</p>
 *
 * <p>Verification strategy:
 * <ol>
 *   <li>Before commit: generate a deterministic token from session's queryId + attemptId</li>
 *   <li>Check if any recent snapshot already carries this token</li>
 *   <li>If found: skip the commit (idempotent no-op)</li>
 *   <li>If not found: proceed, storing the token in the new snapshot's summary</li>
 * </ol>
 *
 * <p>This complements {@link CommitStatusVerifier} which handles the timeout path:
 * when a commit times out, the verifier checks whether the commit actually landed
 * on the catalog side before deciding to retry.</p>
 */
final class IdempotentCommitController {

    private static final Logger LOG = LoggerFactory.getLogger(IdempotentCommitController.class);

    /** Iceberg snapshot summary key for the commit token. */
    static final String COMMIT_TOKEN_KEY = "pangrova.commit-token";

    /** Maximum number of recent snapshots to scan for duplicate tokens. */
    static final int MAX_SNAPSHOT_CHECK_DEPTH = 20;

    private IdempotentCommitController() {
    }

    /**
     * Generates a deterministic commit token from session properties.
     *
     * <p>The token is formed as {@code queryId:attemptId}, where attemptId
     * defaults to "0" if not explicitly set. This ensures that each unique
     * query attempt produces a unique token, and retries with the same
     * queryId+attemptId produce the same token for idempotency.</p>
     *
     * @param session the connector session (must contain queryId in properties)
     * @return the commit token, or null if queryId is not available
     */
    static String generateToken(ConnectorSession session) {
        if (session == null) {
            return null;
        }
        Map<String, String> props = session.getProperties();
        if (props == null) {
            return null;
        }
        String queryId = props.get("queryId");
        if (queryId == null || queryId.isEmpty()) {
            return null;
        }
        String attemptId = props.getOrDefault("attemptId", "0");
        return queryId + ":" + attemptId;
    }

    /**
     * Checks if a commit with the given token has already been applied to the table.
     *
     * <p>Refreshes the table metadata and scans backward through the most recent
     * snapshots (up to {@link #MAX_SNAPSHOT_CHECK_DEPTH}) looking for a matching
     * {@link #COMMIT_TOKEN_KEY} in the snapshot summary.</p>
     *
     * <p>On any error during the check, returns {@code false} (proceed with commit).
     * A false-negative (missed duplicate) is less dangerous than a false-positive
     * (skipping a commit that never landed), because the retry will simply create
     * the snapshot that should have been there.</p>
     *
     * @param table the Iceberg table to check (will be refreshed)
     * @param token the commit token to look for
     * @return true if a snapshot with this token already exists
     */
    static boolean isAlreadyCommitted(Table table, String token) {
        if (token == null || token.isEmpty()) {
            return false;
        }
        try {
            table.refresh();
            return snapshotChainContainsToken(table, table.currentSnapshot(), token);
        } catch (Exception e) {
            // On check failure, proceed with the commit (safer than skipping)
            LOG.warn("event=idempotent_check_failed token={} error={}", token, e.getMessage());
        }
        return false;
    }

    private static boolean snapshotChainContainsToken(Table table, Snapshot start, String token) {
        if (start == null) {
            return false;
        }
        // Walk backward through snapshot parent chain
        Snapshot walk = start;
        for (int i = 0; i < MAX_SNAPSHOT_CHECK_DEPTH && walk != null; i++) {
            if (snapshotHasToken(walk, token)) {
                LOG.info("event=idempotent_commit_detected token={} snapshotId={}",
                        token, walk.snapshotId());
                return true;
            }
            Long parentId = walk.parentId();
            if (parentId == null) {
                break;
            }
            walk = table.snapshot(parentId);
        }
        return false;
    }

    private static boolean snapshotHasToken(Snapshot snapshot, String token) {
        Map<String, String> summary = snapshot.summary();
        return summary != null && token.equals(summary.get(COMMIT_TOKEN_KEY));
    }
}
