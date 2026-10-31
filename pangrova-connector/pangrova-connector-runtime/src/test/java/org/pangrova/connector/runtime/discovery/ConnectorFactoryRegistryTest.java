package org.pangrova.connector.runtime.discovery;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.spi.plugin.ConnectorContext;
import org.pangrova.connector.spi.plugin.ConnectorFactory;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorFactoryRegistryTest {

    @Test
    void indexesFactoriesByName() {
        ConnectorFactory iceberg = new TestFactory("iceberg");
        ConnectorFactory paimon = new TestFactory("paimon");

        ConnectorFactoryRegistry registry = ConnectorFactoryRegistry.of(List.of(iceberg, paimon));

        assertThat(registry.names()).containsExactlyInAnyOrder("iceberg", "paimon");
        assertThat(registry.find("iceberg")).containsSame(iceberg);
        assertThat(registry.require("paimon")).isSameAs(paimon);
    }

    @Test
    void rejectsDuplicateFactoryNames() {
        assertThatThrownBy(() -> ConnectorFactoryRegistry.of(
                List.of(new TestFactory("iceberg"), new TestFactory("iceberg"))))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Duplicate ConnectorFactory name 'iceberg'");
    }

    @Test
    void rejectsBlankFactoryNames() {
        assertThatThrownBy(() -> ConnectorFactoryRegistry.of(List.of(new TestFactory(" "))))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("name must not be blank");
    }

    @Test
    void requireFailsClosedForUnknownProvider() {
        ConnectorFactoryRegistry registry =
                ConnectorFactoryRegistry.of(List.of(new TestFactory("iceberg")));

        assertThat(registry.find("paimon")).isEmpty();
        assertThatThrownBy(() -> registry.require("paimon"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("No ConnectorFactory found for provider 'paimon'");
    }

    private static final class TestFactory implements ConnectorFactory {

        private final String name;

        private TestFactory(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config,
                                ConnectorContext context) {
            throw new UnsupportedOperationException("not used");
        }
    }
}
