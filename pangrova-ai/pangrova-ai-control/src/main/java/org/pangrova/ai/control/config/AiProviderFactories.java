package org.pangrova.ai.control.config;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.pangrova.ai.api.provider.AiProviderFactory;

/** Immutable factory inventory for one configuration parse or Worker registry. */
public final class AiProviderFactories {
    private final Map<String, AiProviderFactory> factories;

    public AiProviderFactories(Iterable<AiProviderFactory> discovered) {
        var byId = new HashMap<String, AiProviderFactory>();
        for (AiProviderFactory factory : discovered) {
            if (factory.apiVersion() != AiProviderFactory.API_VERSION) {
                throw new IllegalArgumentException("Unsupported AI factory API version " + factory.apiVersion()
                        + " for " + factory.getClass().getName());
            }
            var dialects = Set.copyOf(factory.dialects());
            if (dialects.isEmpty()) throw new IllegalArgumentException("AI factory has no dialect");
            for (String id : dialects) {
                if (id.isBlank() || byId.putIfAbsent(id, factory) != null) {
                    throw new IllegalArgumentException("Blank or duplicate AI provider dialect: " + id);
                }
            }
        }
        factories = Map.copyOf(byId);
    }

    public static AiProviderFactories discover() {
        return new AiProviderFactories(ServiceLoader.load(AiProviderFactory.class, AiProviderFactories.class.getClassLoader()));
    }

    public AiProviderFactory require(String dialect, String modelName) {
        var factory = factories.get(dialect);
        if (factory == null) throw new IllegalArgumentException(
                "Unsupported AI provider '" + dialect + "' for model '" + modelName + "'");
        return factory;
    }
}
