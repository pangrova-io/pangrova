package org.pangrova.coordinator.materialization;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.port.FenceValidator;
import org.pangrova.materialization.port.LeaseService;
import org.pangrova.spi.coordination.ExecutionAnchor;

/**
 * Fence adapter backed by the query execution anchor and the live control term.
 *
 * <p>A fence never outlives the anchor's current lease: the repository checks
 * {@code anchor.expires_at >= lease_expires_at} when the attempt is reserved.
 * Publication additionally requires the anchor and leader lease to be live, so
 * a fence whose query was taken over can never publish.
 */
public final class CoordinatorExecutionFence implements LeaseService, FenceValidator {

    private final HighAvailabilityCoordinationService coordination;
    private final String queryId;
    private final LongSupplier leaderEpoch;

    public CoordinatorExecutionFence(HighAvailabilityCoordinationService coordination,
            String queryId, LongSupplier leaderEpoch) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.queryId = Objects.requireNonNull(queryId, "queryId");
        this.leaderEpoch = Objects.requireNonNull(leaderEpoch, "leaderEpoch");
    }

    @Override
    public ExecutionFence issueExecutionFence(MaterializationId object, Duration leaseDuration) {
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw MaterializationException.requestRejected("execution lease must be positive");
        }
        ExecutionAnchor anchor = coordination.requireExecutionAnchor(queryId);
        Instant now = Instant.now();
        Instant requested = now.plus(leaseDuration);
        Instant expiresAt = requested.isAfter(anchor.expiresAt()) ? anchor.expiresAt() : requested;
        if (!expiresAt.isAfter(now)) {
            throw MaterializationException.fenceRejected(
                    "execution anchor " + queryId + " has no remaining lease for " + object.canonicalName());
        }
        long epoch = leaderEpoch.getAsLong();
        if (epoch <= 0) {
            throw MaterializationException.fenceRejected(
                    "no live coordinator control epoch for materialization " + object.canonicalName());
        }
        return new ExecutionFence(anchor.executionId(), epoch, expiresAt);
    }

    @Override
    public void requireValid(ExecutionFence fence, Instant now) {
        ExecutionAnchor anchor = coordination.requireExecutionAnchor(queryId);
        if (!anchor.executionId().equals(fence.executionAnchor())) {
            throw MaterializationException.fenceRejected(
                    "fence anchor " + fence.executionAnchor() + " does not match query " + queryId);
        }
        if (fence.isExpiredAt(now)) {
            throw MaterializationException.fenceRejected(
                    "execution fence expired at " + fence.leaseExpiresAt() + " for " + queryId);
        }
        long epoch = leaderEpoch.getAsLong();
        if (epoch <= 0 || epoch != fence.controlEpoch()) {
            throw MaterializationException.fenceRejected(
                    "execution fence control epoch " + fence.controlEpoch()
                            + " is not the live coordinator term " + epoch);
        }
        if (!coordination.validateDurableExecutionToken(queryId, anchor.fencingToken())) {
            throw MaterializationException.fenceRejected(
                    "execution anchor " + queryId + " is no longer the durable owner");
        }
    }
}
