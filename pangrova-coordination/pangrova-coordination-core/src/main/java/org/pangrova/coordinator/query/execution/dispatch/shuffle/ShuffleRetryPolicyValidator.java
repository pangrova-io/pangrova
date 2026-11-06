package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.util.Objects;

import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;

import static org.pangrova.shuffle.api.capability.ShuffleCapability.CONSUMER_RETRY;

/**
 * Checks an explicit consumer-retry requirement against effective exchange guarantees.
 *
 * <p>The caller must already intersect implementation, deployment and current exchange evidence.
 * This helper is not yet assembled into production dispatch; it does not validate receipts,
 * select attempts or infer guarantees from a storage tier or transport mode.
 */
public final class ShuffleRetryPolicyValidator {
    private ShuffleRetryPolicyValidator() {}

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
