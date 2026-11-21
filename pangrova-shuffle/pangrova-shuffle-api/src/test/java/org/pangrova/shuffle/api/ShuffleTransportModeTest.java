package org.pangrova.shuffle.api;

import org.junit.jupiter.api.Test;

import org.pangrova.shuffle.api.enums.ShuffleTransportMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Transport direction vocabulary; it no longer derives reliability guarantees.
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
    void allValuesEnumerated() {
        assertThat(ShuffleTransportMode.values())
                .containsExactly(ShuffleTransportMode.PULL, ShuffleTransportMode.PUSH);
    }
}
