package org.pangrova.materialization.port;

import org.pangrova.connector.spi.materialization.ChangeProof;
import org.pangrova.connector.spi.materialization.ChangeSetMode;
import org.pangrova.connector.spi.materialization.ChangeSetPlan;
import org.pangrova.connector.spi.materialization.PinnedSource;
import org.pangrova.connector.spi.materialization.SourceRef;
import org.pangrova.connector.spi.materialization.SourceSnapshotRef;

/**
 * Physical holds on exact source snapshots for one attempt.
 *
 * <p>The implementation resolves the connector that owns the source and
 * delegates to its {@code SourceProvider}. A provider that cannot pin the exact
 * planned snapshot must fail; it must never substitute the current snapshot.
 *
 * <p>Both operations must be idempotent for the same pin owner so a retried
 * settlement or a recovery sweep can release an already-released hold.
 */
public interface SourcePinning {

    /**
     * Acquires an exact snapshot hold.
     *
     * @param tenant tenant on whose behalf the source is read
     * @param source source table
     * @param expected exact snapshot observed while planning
     * @param pinOwner durable owner of the hold, normally the attempt id
     * @return the acquired hold, which must match every input exactly
     */
    PinnedSource pin(String tenant, SourceRef source, SourceSnapshotRef expected, String pinOwner);

    /**
     * Plans the change set between two pinned snapshots of the same source.
     *
     * @param from base snapshot hold
     * @param to target snapshot hold
     * @param mode requested change-set mode
     * @return connector change-set plan
     */
    ChangeSetPlan changeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode);

    /**
     * Verifies that a change-set plan was fully evaluated.
     *
     * @param plan plan to verify
     * @return change proof; callers must check {@link ChangeProof#complete()}
     */
    ChangeProof verify(ChangeSetPlan plan);

    /**
     * Releases a hold previously returned by {@link #pin}.
     *
     * @param pinned hold to release
     */
    void unpin(PinnedSource pinned);
}
