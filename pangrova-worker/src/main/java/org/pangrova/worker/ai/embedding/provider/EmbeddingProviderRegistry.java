package org.pangrova.worker.ai.embedding.provider;

import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.ai.embedding.SecretResolver;
import org.pangrova.worker.ai.embedding.EmbeddingProviderLookup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Manages embedding provider instances by model name.
 *
 * <p>Creates providers lazily from model descriptors on first access.
 * Thread-safe: uses ConcurrentHashMap for instance caching.</p>
 */
public final class EmbeddingProviderRegistry implements EmbeddingProviderLookup {

    private final Function<String, EmbeddingModelDescriptor> descriptorLookup;
    private final SecretResolver secretResolver;
    private final OpenAiCompatibleProviderFactory factory;
    private final ConcurrentMap<String, EmbeddingProvider> providers;

    /**
     * Creates a registry that resolves model descriptors and secrets on demand.
     *
     * @param descriptorLookup function to retrieve model descriptors by name
     * @param secretResolver   resolver for API key secret references
     * @param factory          factory for creating OpenAI-compatible providers
     */
    public EmbeddingProviderRegistry(
            Function<String, EmbeddingModelDescriptor> descriptorLookup,
            SecretResolver secretResolver,
            OpenAiCompatibleProviderFactory factory) {
        if (descriptorLookup == null) {
            throw new IllegalArgumentException("descriptorLookup required");
        }
        if (secretResolver == null) {
            throw new IllegalArgumentException("secretResolver required");
        }
        if (factory == null) {
            throw new IllegalArgumentException("factory required");
        }
        this.descriptorLookup = descriptorLookup;
        this.secretResolver = secretResolver;
        this.factory = factory;
        this.providers = new ConcurrentHashMap<>();
    }

    @Override
    public EmbeddingProvider resolve(String modelName) {
        if (modelName == null || modelName.isBlank()) {
            return null;
        }
        return providers.computeIfAbsent(modelName, this::createProvider);
    }

    /**
     * Invalidates the cached provider for the given model name.
     *
     * <p>Used when a model configuration is updated (e.g., ALTER MODEL)
     * to force re-creation on next access.</p>
     *
     * @param modelName the model name to invalidate
     */
    public void invalidate(String modelName) {
        if (modelName != null) {
            providers.remove(modelName);
        }
    }

    private EmbeddingProvider createProvider(String modelName) {
        EmbeddingModelDescriptor descriptor = descriptorLookup.apply(modelName);
        if (descriptor == null) {
            throw new IllegalStateException(
                    "No model descriptor found for model: " + modelName);
        }
        if (!factory.supports(descriptor.provider())) {
            throw new IllegalStateException(
                    "Unsupported provider type: " + descriptor.provider()
                            + " for model: " + modelName);
        }
        String apiKey = secretResolver.resolve(descriptor.apiKeySecret());
        return factory.create(descriptor, apiKey);
    }
}
