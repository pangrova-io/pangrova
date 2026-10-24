package org.pangrova.materialization.domain;

import java.time.Instant;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;

/**
 * Durable pin of one source snapshot for the lifetime of an attempt.
 *
 * @param attempt owning attempt
 * @param source pinned source
 * @param snapshot pinned snapshot
 * @param expiresAt instant after which the pin may be reclaimed
 * @param state pin state
 */
public record SourcePin(
        AttemptId attempt,
        SourceRef source,
        SourceSnapshotRef snapshot,
        Instant expiresAt,
        PinState state) {

    public SourcePin {
        if (attempt == null || source == null || snapshot == null) {
            throw new IllegalArgumentException("attempt, source, and snapshot must not be null");
        }
        if (!source.connectorId().equals(snapshot.connectorId())) {
            throw new IllegalArgumentException("snapshot connector does not match source connector");
        }
        if (!snapshot.pinnable()) {
            throw new IllegalArgumentException("cannot pin a non-pinnable snapshot");
        }
        if (expiresAt == null) {
            throw new IllegalArgumentException("expiresAt must not be null");
        }
        if (state == null) {
            throw new IllegalArgumentException("state must not be null");
        }
    }

    /**
     * Creates an acquired pin.
     *
     * @param attempt owning attempt
     * @param source pinned source
     * @param snapshot pinned snapshot
     * @param expiresAt expiry instant
     * @return acquired pin
     */
    public static SourcePin acquired(
            AttemptId attempt, SourceRef source, SourceSnapshotRef snapshot, Instant expiresAt) {
        return new SourcePin(attempt, source, snapshot, expiresAt, PinState.ACQUIRED);
    }

    /**
     * Marks the pin released after the attempt settles.
     *
     * @return released pin
     */
    public SourcePin release() {
        return withState(PinState.RELEASED);
    }

    /**
     * Marks the pin expired after its deadline passed.
     *
     * @return expired pin
     */
    public SourcePin expire() {
        return withState(PinState.EXPIRED);
    }

    /**
     * Returns whether the pin still protects the source snapshot.
     *
     * @param now current instant
     * @return true when acquired and not expired
     */
    public boolean isActiveAt(Instant now) {
        return state == PinState.ACQUIRED && expiresAt.isAfter(now);
    }

    private SourcePin withState(PinState target) {
        if (state != PinState.ACQUIRED) {
            throw MaterializationException.invalidStateTransition(
                    "source pin for " + source.canonicalName() + " is already " + state);
        }
        return new SourcePin(attempt, source, snapshot, expiresAt, target);
    }
}
