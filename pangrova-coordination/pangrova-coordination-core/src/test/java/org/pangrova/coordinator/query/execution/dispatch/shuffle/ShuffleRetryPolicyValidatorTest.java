package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.shuffle.api.ShuffleCapabilities;
import org.pangrova.shuffle.api.ShuffleDurability;
import org.junit.jupiter.api.Test;

/**
 * ADR-002 D3 acceptance: task-level retry is bound to {@link ShuffleCapabilities#supportsTaskRetry()}
 * and fails fast on an EPHEMERAL provider instead of silently degrading to whole-query retry.
 */
class ShuffleRetryPolicyValidatorTest {

    @Test
    void taskRetryOnEphemeralProviderFailsFast() {
        ShuffleCapabilities caps = ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL);
        assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validate(true, caps))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("EPHEMERAL")
                .hasMessageContaining("task retry");
    }

    @Test
    void taskRetryOnLocalSpillProviderIsAccepted() {
        ShuffleCapabilities caps = ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL);
        assertThatCode(() -> ShuffleRetryPolicyValidator.validate(true, caps))
                .doesNotThrowAnyException();
    }

    @Test
    void taskRetryOnDurableProviderIsAccepted() {
        ShuffleCapabilities caps = ShuffleCapabilities.of(ShuffleDurability.DURABLE);
        assertThatCode(() -> ShuffleRetryPolicyValidator.validate(true, caps))
                .doesNotThrowAnyException();
    }

    @Test
    void queryLevelRetryIsAcceptedOnEveryDurabilityTier() {
        for (ShuffleDurability durability : ShuffleDurability.values()) {
            ShuffleCapabilities caps = ShuffleCapabilities.of(durability);
            assertThatCode(() -> ShuffleRetryPolicyValidator.validate(false, caps))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void nullCapabilitiesRejected() {
        assertThatThrownBy(() -> ShuffleRetryPolicyValidator.validate(true, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void bindsToCapabilityNotProviderName() {
        // A custom EPHEMERAL provider that nonetheless claims task-retry support is honored:
        // the validator reads supportsTaskRetry(), never the durability tier directly.
        ShuffleCapabilities lyingButCapable = new ShuffleCapabilities() {
            @Override
            public ShuffleDurability durability() {
                return ShuffleDurability.EPHEMERAL;
            }

            @Override
            public boolean supportsTaskRetry() {
                return true;
            }
        };
        assertThatCode(() -> ShuffleRetryPolicyValidator.validate(true, lyingButCapable))
                .doesNotThrowAnyException();
    }
}
