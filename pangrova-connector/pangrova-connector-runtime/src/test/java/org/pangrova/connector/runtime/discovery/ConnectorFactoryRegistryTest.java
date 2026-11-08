package org.pangrova.connector.runtime.discovery;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.connector.spi.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.spi.plugin.ConnectorFactory;
import org.pangrova.connector.spi.plugin.ConnectorRole;

import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @Test
    void rejectsIncompatibleApiVersion() {
        assertThatThrownBy(() -> ConnectorFactoryRegistry.of(
                List.of(new VersionedFactory("future", 99))))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("declares API version 99");
    }

    @Test
    void filtersFactoriesByProcessRole() {
        ConnectorFactory coordinatorOnly = new RoleFactory("coordinator-only", Set.of(ConnectorRole.COORDINATOR));
        ConnectorFactory workerOnly = new RoleFactory("worker-only", Set.of(ConnectorRole.WORKER));

        ConnectorFactoryRegistry coordinator = ConnectorFactoryRegistry.of(
                List.of(coordinatorOnly, workerOnly), ConnectorRole.COORDINATOR);
        ConnectorFactoryRegistry worker = ConnectorFactoryRegistry.of(
                List.of(coordinatorOnly, workerOnly), ConnectorRole.WORKER);

        assertThat(coordinator.names()).containsExactly("coordinator-only");
        assertThat(worker.names()).containsExactly("worker-only");
    }

    @Test
    void validatesConfigurationBeforeCreation() {
        ConnectorFactoryRegistry registry = ConnectorFactoryRegistry.of(
                List.of(new ValidatingFactory("validated")));

        assertThatThrownBy(() -> registry.validateConfiguration("validated", Map.of("bad", "value")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("bad configuration");
    }

    private static final class VersionedFactory extends TestFactory {

        private final int version;

        private VersionedFactory(String name, int version) {
            super(name);
            this.version = version;
        }

        @Override
        public int apiVersion() {
            return version;
        }
    }

    private static final class RoleFactory extends TestFactory {

        private final Set<ConnectorRole> roles;

        private RoleFactory(String name, Set<ConnectorRole> roles) {
            super(name);
            this.roles = roles;
        }

        @Override
        public Set<ConnectorRole> roles() {
            return roles;
        }
    }

    private static final class ValidatingFactory extends TestFactory {

        private ValidatingFactory(String name) {
            super(name);
        }

        @Override
        public void validateConfiguration(Map<String, String> config) {
            if (config.containsKey("bad")) {
                throw new ConfigurationException(
                        org.pangrova.common.exception.PangrovaErrorCode.CONFIG_INVALID_VALUE,
                        "bad configuration");
            }
        }
    }

    private static class TestFactory implements ConnectorFactory {

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
