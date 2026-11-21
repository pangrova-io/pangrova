package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.connector.api.constants.ConnectorFactoryContract;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.enums.ConnectorRole;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.HealthCheckResult;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.api.plugin.ConnectorFactory;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/** Packaged by ExternalCatalogRolesIntegrationTest. Its resource comes only from its plugin JAR. */
public final class ExternalCatalogTestFactory implements ConnectorFactory {
    @Override public String getName() { return "external-test"; }
    @Override public int apiVersion() { return ConnectorFactoryContract.CURRENT_API_VERSION; }
    @Override public Set<ConnectorRole> roles() { return Set.of(ConnectorRole.WORKER, ConnectorRole.COORDINATOR); }

    @Override
    public void validateConfiguration(Map<String, String> config) {
        if (Thread.currentThread().getContextClassLoader() != getClass().getClassLoader()) {
            throw new IllegalStateException("Expected role plugin context");
        }
    }

    @Override
    public Connector create(String name, Map<String, String> config, ConnectorContext context) {
        validateConfiguration(config);
        return new ExternalCatalog(Path.of(config.get("events")));
    }

    /** A dependency from the second plugin JAR that is required when the Worker opens a revision. */
    public static final class ExternalCodec implements ConnectorOutputHandleCodec {
        @Override public String typeUrl() { return "external-test"; }
        @Override public int codecVersion() { return 1; }
        @Override public byte[] serialize(ConnectorOutputTableHandle value) { throw new UnsupportedOperationException(); }
        @Override public ConnectorOutputTableHandle deserialize(int version, byte[] bytes) { throw new UnsupportedOperationException(); }
        @Override public java.util.List<String> writeColumnNames(ConnectorOutputTableHandle value) { throw new UnsupportedOperationException(); }
    }

    public record ExternalCatalog(Path events) implements Connector {
        @Override public ConnectorMetadata getMetadata(ConnectorSession session) { throw new UnsupportedOperationException(); }
        @Override public ConnectorSplitManager getSplitManager() { throw new UnsupportedOperationException(); }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { throw new UnsupportedOperationException(); }
        @Override public ConnectorOutputHandleCodec getOutputHandleCodec() { return new ExternalCodec(); }

        @Override
        public HealthCheckResult healthCheck() {
            return getClass().getClassLoader().getResource("external-catalog-marker") == null
                    ? HealthCheckResult.unhealthy("plugin closed") : HealthCheckResult.healthy();
        }

        @Override
        public void shutdown() {
            try (var marker = getClass().getResourceAsStream("/external-catalog-marker")) {
                if (marker == null) throw new IllegalStateException("Plugin closed before its catalog");
                Files.writeString(events, new String(marker.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8),
                        java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }
    }
}
