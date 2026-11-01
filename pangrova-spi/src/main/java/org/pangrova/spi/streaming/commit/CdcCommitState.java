package org.pangrova.spi.streaming.commit;

/**
 * State machine for a CDC two-phase commit transaction.
 *
 * <p>Transitions:</p>
 * <pre>
 *   PREPARED → COMMITTING → COMMITTED
 *   PREPARED → ROLLING_BACK → ROLLED_BACK
 * </pre>
 *
 * <p>Recovery semantics:</p>
 * <ul>
 *   <li>PREPARED past deadline → rollback</li>
 *   <li>COMMITTING → quarantine until the external catalog outcome is reconciled</li>
 *   <li>ROLLING_BACK → retry rollback (idempotent)</li>
 * </ul>
 */
public enum CdcCommitState {

    /** Source-batch intent exists; external publication has not started. */
    PREPARED,

    /** Phase 2 in progress: commit has been initiated. */
    COMMITTING,

    /** Terminal: ordered participant commits completed and state was acknowledged. */
    COMMITTED,

    /** Rollback in progress. */
    ROLLING_BACK,

    /** Terminal: prepare was abandoned before external publication. */
    ROLLED_BACK
}
