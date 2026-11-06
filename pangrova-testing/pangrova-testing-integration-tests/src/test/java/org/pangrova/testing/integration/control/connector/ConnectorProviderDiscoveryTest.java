package org.pangrova.testing.integration.control.connector;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.spi.plugin.ConnectorFactory;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads real provider service descriptors on the assembled integration classpath. */
class ConnectorProviderDiscoveryTest {
    @Test
    void discoversEachProductionProviderThroughTheUnifiedContract() {
        var providers = ServiceLoader.load(ConnectorFactory.class).stream()
                .map(ServiceLoader.Provider::get).toList();
        assertThat(providers).extracting(ConnectorFactory::getName)
                .containsExactlyInAnyOrder("iceberg", "paimon", "lance");
        assertThat(providers).extracting(value -> value.getClass().getName()).doesNotHaveDuplicates();
        assertThat(ConnectorFactory.class.getName()).isEqualTo("org.pangrova.connector.spi.plugin.ConnectorFactory");
    }
}
