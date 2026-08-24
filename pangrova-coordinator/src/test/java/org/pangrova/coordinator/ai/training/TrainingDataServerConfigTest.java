package org.pangrova.coordinator.ai.training;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Task 162: TrainingDataServerConfig must default to disabled
 * to respect the pure control-plane principle.
 */
class TrainingDataServerConfigTest {

    @Test
    @DisplayName("defaults() returns config with enabled=false")
    void defaultsDisabled() {
        TrainingDataServerConfig config = TrainingDataServerConfig.defaults();
        assertThat(config.enabled()).isFalse();
        assertThat(config.port()).isEqualTo(TrainingDataServerConfig.DEFAULT_PORT);
        assertThat(config.maxConcurrentStreams()).isEqualTo(TrainingDataServerConfig.DEFAULT_MAX_CONCURRENT_STREAMS);
    }

    @Test
    @DisplayName("explicit enabled=true allows construction")
    void explicitEnabled() {
        TrainingDataServerConfig config = new TrainingDataServerConfig(
                true, 9090, 50, "jwt", 65536);
        assertThat(config.enabled()).isTrue();
    }

    @Test
    @DisplayName("invalid port rejected")
    void invalidPort() {
        assertThatThrownBy(() -> new TrainingDataServerConfig(false, 0, 50, "jwt", 65536))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
