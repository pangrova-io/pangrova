package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageBackendFactory;

import java.util.List;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LineageBackendConfigurationTest {
    @Test
    void installedProvidersAdvertiseDistinctDeliveryRequirements() {
        var factories = ServiceLoader.load(LineageBackendFactory.class).stream()
                .map(ServiceLoader.Provider::get).toList();
        assertThat(factories).extracting(LineageBackendFactory::providerId)
                .containsExactlyInAnyOrder("LOG", "HTTP");
        try (var log = LineageBackendConfiguration.createEmitter("LOG", "ignored", null, factories)) {
            assertThat(log.namespace()).isEqualTo("pangrova");
        }
        assertThatThrownBy(() -> LineageBackendConfiguration.createEmitter("HTTP", "cluster", null, factories))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("metadata outbox");
        assertThatThrownBy(() -> LineageBackendConfiguration.createEmitter("missing", "cluster", null, factories))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown lineage backend");
    }

    @Test
    void rejectsInvalidRegistrationsBeforeCreatingAnyBackend() {
        var factory = new UnopenedFactory("LOG", LineageBackendFactory.API_VERSION);
        assertThatThrownBy(() -> LineageBackendConfiguration.createEmitter("LOG", "ns", null, List.of(factory, factory)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
        for (var invalid : List.of(new UnopenedFactory(" ", 1), new UnopenedFactory("LOG", 2))) {
            assertThatThrownBy(() -> LineageBackendConfiguration.createEmitter("LOG", "ns", null, List.of(invalid)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid lineage backend factory");
        }
    }

    private record UnopenedFactory(String providerId, int apiVersion) implements LineageBackendFactory {
        @Override public Delivery delivery() { return Delivery.TRANSIENT; }
        @Override public LineageBackend create(String namespace) { throw new AssertionError("invalid factory used"); }
    }
}
