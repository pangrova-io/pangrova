package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.Objects;

import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;

import static org.pangrova.shuffle.api.enums.ShuffleCapability.CONSUMER_RETRY;
import static org.pangrova.shuffle.api.enums.ShuffleCapability.SPECULATIVE_ATTEMPT_ISOLATION;

/**
 * Checks an explicit consumer-retry requirement against effective exchange guarantees.
 *
 * <p>The caller must already intersect implementation, deployment and current exchange evidence.
 * Current dispatch rejects consumer replay because no deployed provider can renew read authority.
 * Speculation is checked during assembly. Receipt validation and attempt selection belong to publication.
 */
public final class ShuffleRetryPolicyValidator {
    private ShuffleRetryPolicyValidator() {}

    /** Rejects speculative execution at dispatch assembly when the selected backend cannot isolate its winner. */
    public static void validateSpeculation(boolean enabled, ShuffleCapabilityDescriptor capabilities) {
        Objects.requireNonNull(capabilities, "capabilities");
        if (enabled && !capabilities.supports(SPECULATIVE_ATTEMPT_ISOLATION)) {
            throw new IllegalArgumentException("Speculation requires explicit winning producer attempts from the Shuffle provider");
        }
    }

    /**
     * Rejects unsupported consumer retry without silently choosing a different retry policy.
     *
     * @param consumerRetryRequested whether retry of a consumer against the same committed input is required
     * @param capabilities effective guarantees for the particular exchange
     * @throws NullPointerException if capabilities is null
     * @throws IllegalStateException if the required consumer retry guarantee is absent
     */
    public static void validateConsumerRetry(boolean consumerRetryRequested, ShuffleCapabilityDescriptor capabilities) {
        Objects.requireNonNull(capabilities, "capabilities must not be null");
        if (consumerRetryRequested && !capabilities.supports(CONSUMER_RETRY)) {
            throw new IllegalStateException(
                    "Consumer retry requires the effective Shuffle capability CONSUMER_RETRY; "
                            + "storage medium and transport direction do not establish this guarantee. "
                            + "Current capabilities: " + capabilities.capabilities());
        }
    }
}
