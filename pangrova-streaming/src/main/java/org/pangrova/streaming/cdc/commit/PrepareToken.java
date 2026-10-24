package org.pangrova.streaming.cdc.commit;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Token issued by {@link CdcCommitCoordinator#prepare} representing a prepared
 * two-phase commit transaction.
 *
 * <p>The token carries all information needed to commit or rollback the transaction:
 * the unique transaction id, the source identity, the offset range being committed,
 * and a deadline after which the prepared state is considered stale and eligible
 * for automatic rollback during recovery.</p>
 *
 * @param id       unique transaction identifier
 * @param source   the CDC source this transaction belongs to
 * @param checkpoint every exact source range being committed
 * @param deadline absolute time after which this prepared transaction expires
 */
public record PrepareToken(UUID id, SourceId source, CdcBatchCheckpoint checkpoint, Instant deadline) {

    public PrepareToken {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        Objects.requireNonNull(deadline, "deadline must not be null");
    }
}
