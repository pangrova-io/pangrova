package org.pangrova.materialization.api.domain;

import java.time.Duration;
import java.time.Instant;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Reader-held pin that prevents garbage collection of a generation.
 *
 * <p>Queries lease generations used in scans. Incremental refresh attempts lease their base
 * generation while computing and publishing. Each owner releases only its own leases.
 *
 * @param generation pinned generation
 * @param reader owning query or refresh attempt
 * @param owner principal or attempt label for audit
 * @param expiresAt instant after which the pin no longer protects the generation
 * @param heartbeatAt last heartbeat instant
 */
public record ReaderPin(
        GenerationId generation,
        ReaderId reader,
        String owner,
        Instant expiresAt,
        Instant heartbeatAt) {

    public ReaderPin {
        if (generation == null || reader == null) {
            throw new IllegalArgumentException("generation and reader must not be null");
        }
        owner = requireText(owner, "owner");
        if (expiresAt == null || heartbeatAt == null) {
            throw new IllegalArgumentException("expiresAt and heartbeatAt must not be null");
        }
        if (expiresAt.isBefore(heartbeatAt)) {
            throw new IllegalArgumentException("expiresAt must not precede heartbeatAt");
        }
    }

    /**
     * Renews the pin.
     *
     * @param leaseDuration new lease duration
     * @param now heartbeat instant
     * @return renewed pin
     */
    public ReaderPin renew(Duration leaseDuration, Instant now) {
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("lease duration must be positive");
        }
        return new ReaderPin(generation, reader, owner, now.plus(leaseDuration), now);
    }

    /**
     * Returns whether the pin still protects the generation.
     *
     * @param now current instant
     * @return true when not expired
     */
    public boolean isActiveAt(Instant now) {
        return expiresAt.isAfter(now);
    }
}
