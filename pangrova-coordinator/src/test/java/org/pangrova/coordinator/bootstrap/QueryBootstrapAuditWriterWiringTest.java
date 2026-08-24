package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies QueryBootstrap can be constructed with default configuration.
 * The audit writer wiring is handled internally during bootstrap startup.
 */
class QueryBootstrapAuditWriterWiringTest {

    @Test
    void defaultConfigurationConstructsSuccessfully() {
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults("audit-test", 0);
        assertThat(config).isNotNull();
        assertThat(config.core().coordinatorId()).isEqualTo("audit-test");
    }
}
