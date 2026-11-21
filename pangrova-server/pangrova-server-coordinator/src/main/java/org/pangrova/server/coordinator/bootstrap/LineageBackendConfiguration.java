package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.governance.api.enums.LineageDeliveryMode;
import org.pangrova.governance.api.lineage.LineageBackendFactory;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.persistence.query.api.lineage.QueryLineageOutbox;

import java.util.HashMap;
import java.util.Objects;
import java.util.ServiceLoader;

/** Selects the configured Coordinator lineage transport and binds it to the required delivery mode. */
final class LineageBackendConfiguration {
    private LineageBackendConfiguration() {}

    static LineageEventEmitter createEmitter(QueryLineageOutbox outbox) {
        return createEmitter(System.getProperty("pangrova.lineage.backend", "LOG"),
                System.getProperty("pangrova.lineage.namespace", "pangrova"), outbox,
                ServiceLoader.load(LineageBackendFactory.class, LineageBackendConfiguration.class.getClassLoader()));
    }

    static LineageEventEmitter createEmitter(String providerId, String namespace, QueryLineageOutbox outbox,
            Iterable<LineageBackendFactory> factories) {
        var installed = new HashMap<String, LineageBackendFactory>();
        for (var factory : factories) {
            String id = factory.providerId();
            if (id == null || id.isBlank() || factory.apiVersion() != LineageBackendFactory.API_VERSION) {
                throw new IllegalArgumentException("Invalid lineage backend factory " + id);
            }
            if (installed.putIfAbsent(id, factory) != null) {
                throw new IllegalArgumentException("Duplicate lineage backend: " + id);
            }
        }
        var factory = installed.get(providerId);
        if (factory == null) {
            throw new IllegalArgumentException("Unknown lineage backend: " + providerId);
        }
        var mode = Objects.requireNonNull(factory.delivery(), "lineage delivery mode");
        if (mode == LineageDeliveryMode.DURABLE_OUTBOX && outbox == null) {
            throw new IllegalStateException(providerId + " lineage requires the metadata outbox");
        }
        // LOG deployments historically use the engine namespace, independently of HTTP namespace configuration.
        String effectiveNamespace = mode == LineageDeliveryMode.TRANSIENT ? "pangrova" : namespace;
        if (effectiveNamespace == null || effectiveNamespace.isBlank() || effectiveNamespace.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Lineage requires an unambiguous deployment namespace");
        }
        var backend = Objects.requireNonNull(factory.create(effectiveNamespace), "lineage backend");
        try {
            return switch (mode) {
                case TRANSIENT -> new LineageEventEmitter(backend);
                case DURABLE_OUTBOX -> new LineageEventEmitter(backend, outbox, effectiveNamespace);
            };
        } catch (RuntimeException | Error failure) {
            try {
                backend.close();
            } catch (Throwable cleanup) {
                if (cleanup != failure) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }
}
