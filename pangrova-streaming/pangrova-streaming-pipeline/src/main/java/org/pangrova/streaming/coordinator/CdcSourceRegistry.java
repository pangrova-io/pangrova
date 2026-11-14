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

    public CdcSourceRegistry(Iterable<CdcSourceFactory> providers) {
        var registered = new LinkedHashMap<String, CdcSourceFactory>();
        for (CdcSourceFactory factory : providers) {
            String id = factory.identifier();
            if (id == null || !id.matches("[a-z][a-z0-9_]*") || registered.putIfAbsent(id, factory) != null) {
                throw new IllegalArgumentException("Invalid or duplicate CDC source identifier: " + id);
            }
            if (factory.apiVersion() != CdcSourceFactory.API_VERSION) {
                throw new IllegalArgumentException("Incompatible CDC source API for " + id + ": " + factory.apiVersion());
            }
        }
        factories = Map.copyOf(registered);
    }

    public void validate(String id, Map<String, String> properties) {
        require(id).validate(properties);
    }

    public CdcSourceReader open(StreamState state) {
        var provider = require(state.sourceProvider());
        provider.validate(state.sourceProperties());
        return Objects.requireNonNull(provider.open(state.checkpointId(), state.sourceProperties()),
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
