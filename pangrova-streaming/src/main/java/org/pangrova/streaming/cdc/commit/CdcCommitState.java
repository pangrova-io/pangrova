package org.pangrova.streaming.cdc.commit;

/**
 * State machine for a CDC two-phase commit transaction.
 *
 * <p>Transitions:</p>
 * <pre>
 *   PREPARED → COMMITTING → COMMITTED
 *   PREPARED → ROLLING_BACK → ROLLED_BACK
 *   COMMITTING → ROLLING_BACK → ROLLED_BACK  (on commit failure)
 * </pre>
 *
 * <p>Recovery semantics:</p>
 * <ul>
 *   <li>PREPARED past deadline → rollback</li>
 *   <li>COMMITTING → retry commit (idempotent)</li>
 *   <li>ROLLING_BACK → retry rollback (idempotent)</li>
 * </ul>
 */
public enum CdcCommitState {

    /** Phase 1 complete: both refresh and offset are prepared but not committed. */
    PREPARED,

    /** Phase 2 in progress: commit has been initiated. */
    COMMITTING,

    /** Terminal: transaction successfully committed on both sides. */
    COMMITTED,

    /** Rollback in progress. */
    ROLLING_BACK,

    /** Terminal: transaction rolled back on both sides. */
    ROLLED_BACK
}
