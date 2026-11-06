package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.api.capability.ShuffleCapability;
import org.pangrova.shuffle.api.capability.ShuffleCapabilityDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.shuffle.api.capability.ShuffleCapability.*;

class ShuffleRetryPolicyValidatorTest {
    @Test
    void unknownCapabilitiesCannotEnableTaskRetry() {
        assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(true, ShuffleCapabilityDescriptor.none()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("CONSUMER_RETRY");
    }

    @Test
    void onlyExplicitConsumerRetrySatisfiesTheRequirement() {
        for (ShuffleCapability capability : ShuffleCapability.values()) {
            var descriptor = ShuffleCapabilityDescriptor.of(capability);
            if (capability == CONSUMER_RETRY) {
                assertThatCode(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(true, descriptor)).doesNotThrowAnyException();
            } else {
                assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(true, descriptor))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("CONSUMER_RETRY");
            }
        }
    }

    @Test
    void deploymentOrExchangeRestrictionsCannotBeBypassedByProviderSupport() {
        var provider = ShuffleCapabilityDescriptor.of(CONSUMER_RETRY, READ_AFTER_COMPUTE_WORKER_EXIT);
        var deployment = ShuffleCapabilityDescriptor.of(CONSUMER_RETRY);
        var unpublished = ShuffleCapabilityDescriptor.of(PRODUCER_RETRY);
        assertThatCode(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(true, provider.intersect(deployment)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(
                true, provider.intersect(deployment).intersect(unpublished)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(
                true, provider.intersect(ShuffleCapabilityDescriptor.of(READ_AFTER_COMPUTE_WORKER_EXIT))))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void noTaskRetryRequirementDoesNotRequireAnUnrelatedGuarantee() {
        for (ShuffleCapability capability : ShuffleCapability.values()) {
            assertThatCode(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(false, ShuffleCapabilityDescriptor.of(capability)))
                    .doesNotThrowAnyException();
        }
        assertThatCode(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(false, ShuffleCapabilityDescriptor.none()))
                .doesNotThrowAnyException();
    }

    @Test
    void nullCapabilitiesRejectedEvenWithoutRetryRequirement() {
        assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(true, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validateConsumerRetry(false, null)).isInstanceOf(NullPointerException.class);
    }
}
