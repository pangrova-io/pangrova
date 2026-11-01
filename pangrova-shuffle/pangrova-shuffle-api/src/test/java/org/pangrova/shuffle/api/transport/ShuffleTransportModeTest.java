package org.pangrova.shuffle.api.transport;

import org.pangrova.shuffle.api.capability.ShuffleCapabilities;
import org.pangrova.shuffle.api.capability.ShuffleDurability;
import org.pangrova.shuffle.api.capability.ShuffleRetentionPolicy;
import org.pangrova.shuffle.api.transport.ShuffleTransportMode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ShuffleTransportMode} enum and its integration with
 * {@link ShuffleCapabilities}.
 */
class ShuffleTransportModeTest {

    @Test
    void pullModeExists() {
        assertThat(ShuffleTransportMode.PULL).isNotNull();
        assertThat(ShuffleTransportMode.PULL.name()).isEqualTo("PULL");
    }

    @Test
    void pushModeExists() {
        assertThat(ShuffleTransportMode.PUSH).isNotNull();
        assertThat(ShuffleTransportMode.PUSH.name()).isEqualTo("PUSH");
    }

    @Test
    void capabilitiesDefaultsToPull() {
        ShuffleCapabilities caps = ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL);
        assertThat(caps.transportMode()).isEqualTo(ShuffleTransportMode.PULL);
    }

    @Test
    void capabilitiesWithExplicitPush() {
        ShuffleCapabilities caps = ShuffleCapabilities.of(
                ShuffleDurability.DURABLE,
                ShuffleRetentionPolicy.PERSISTENT,
                ShuffleTransportMode.PUSH);
        assertThat(caps.transportMode()).isEqualTo(ShuffleTransportMode.PUSH);
        assertThat(caps.durability()).isEqualTo(ShuffleDurability.DURABLE);
        assertThat(caps.retentionPolicy()).isEqualTo(ShuffleRetentionPolicy.PERSISTENT);
    }

    @Test
    void capabilitiesWithExplicitPull() {
        ShuffleCapabilities caps = ShuffleCapabilities.of(
                ShuffleDurability.LOCAL_SPILL,
                ShuffleRetentionPolicy.DETACHED,
                ShuffleTransportMode.PULL);
        assertThat(caps.transportMode()).isEqualTo(ShuffleTransportMode.PULL);
        assertThat(caps.durability()).isEqualTo(ShuffleDurability.LOCAL_SPILL);
    }

    @Test
    void capabilitiesThreeArgNullTransportThrows() {
        assertThatThrownBy(() -> ShuffleCapabilities.of(
                ShuffleDurability.DURABLE, ShuffleRetentionPolicy.PERSISTENT, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transport");
    }

    @Test
    void capabilitiesThreeArgNullDurabilityThrows() {
        assertThatThrownBy(() -> ShuffleCapabilities.of(
                null, ShuffleRetentionPolicy.PERSISTENT, ShuffleTransportMode.PUSH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("durability");
    }

    @Test
    void capabilitiesThreeArgNullRetentionThrows() {
        assertThatThrownBy(() -> ShuffleCapabilities.of(
                ShuffleDurability.DURABLE, null, ShuffleTransportMode.PUSH))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retention");
    }

    @Test
    void pushProviderSupportsTaskRetry() {
        ShuffleCapabilities caps = ShuffleCapabilities.of(
                ShuffleDurability.DURABLE,
                ShuffleRetentionPolicy.PERSISTENT,
                ShuffleTransportMode.PUSH);
        assertThat(caps.supportsTaskRetry()).isTrue();
        assertThat(caps.supportsSafeScaleDown()).isTrue();
    }

    @Test
    void allValuesEnumerated() {
        assertThat(ShuffleTransportMode.values())
                .containsExactly(ShuffleTransportMode.PULL, ShuffleTransportMode.PUSH);
    }
}
