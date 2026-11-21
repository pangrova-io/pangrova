package org.pangrova.streaming.coordinator;

import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.source.CdcSourceFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/** Holds source factories for one Coordinator. No source client is opened during discovery. */
public final class CdcSourceRegistry {
    private final Map<String, CdcSourceFactory> factories;

    public static CdcSourceRegistry discover(ClassLoader loader) {
        return new CdcSourceRegistry(ServiceLoader.load(CdcSourceFactory.class, loader));
    }

    /** Indexes CDC source factories by identifier, rejecting invalid or duplicate identifiers. */
    public CdcSourceRegistry(Iterable<CdcSourceFactory> providers) {
        var registered = new LinkedHashMap<String, CdcSourceFactory>();
        for (CdcSourceFactory factory : providers) {
            String id = factory.identifier();
            if (id == null || !id.matches("[a-z][a-z0-9_]*") || registered.putIfAbsent(id, factory) != null) {
                throw new IllegalArgumentException("Invalid or duplicate CDC source identifier: " + id);
            }
            if (factory.apiVersion() != CdcSourceFactory.API_VERSION) {
                throw new IllegalArgumentException("Incompatible CDC source API for " + id + ": "
                        + factory.apiVersion());
            }
        }
        factories = Map.copyOf(registered);
    }

    public void validate(String id, Map<String, String> properties) {
        require(id).validate(properties);
    }

    /** Validates the stream source properties and opens a reader at its checkpoint. */
    public CdcSourceReader open(StreamState state) {
        return open(state.sourceProvider(), state.checkpointId(), state.sourceProperties());
    }

    /** Opens an exact task reader from admitted provider properties. */
    public CdcSourceReader open(
            String providerId, String checkpointId, Map<String, String> properties) {
        var provider = require(providerId);
        provider.validate(properties);
        return Objects.requireNonNull(provider.open(checkpointId, properties),
                "CDC source factory returned no reader");
    }

    private CdcSourceFactory require(String id) {
        CdcSourceFactory factory = factories.get(id);
        if (factory == null) {
            throw new IllegalArgumentException("Unsupported CDC source: " + id + ". Available: " + factories.keySet());
        }
        return factory;
    }
}
