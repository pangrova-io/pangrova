package org.pangrova.worker.ai.inference;

import org.pangrova.spi.ai.embedding.SecretResolver;
import org.pangrova.spi.ai.inference.InferenceProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Manages inference provider instances by model name.
 *
 * <p>Creates providers lazily from model configuration on first access.
 * Thread-safe: uses ConcurrentHashMap for instance caching.</p>
 */
public final class InferenceProviderRegistry {

    private final Function<String, InferenceModelConfig> configLookup;
    private final SecretResolver secretResolver;
    private final ConcurrentMap<String, InferenceProvider> providers = new ConcurrentHashMap<>();

    /**
     * Configuration descriptor for an inference model endpoint.
     */
    public record InferenceModelConfig(
            String name,
            String endpoint,
            String apiKeySecret,
            int maxBatchSize,
            double maxRequestsPerSecond,
            int timeoutSeconds) {

        public InferenceModelConfig {
            validateRequiredText(name, "name");
            validateRequiredText(endpoint, "endpoint");
            validateRequiredText(apiKeySecret, "apiKeySecret");
            if (maxBatchSize <= 0) {
                throw new IllegalArgumentException("maxBatchSize must be positive");
            }
            if (maxRequestsPerSecond <= 0) {
                throw new IllegalArgumentException("maxRequestsPerSecond must be positive");
            }
            if (timeoutSeconds <= 0) {
                throw new IllegalArgumentException("timeoutSeconds must be positive");
            }
        }

        private static void validateRequiredText(String value, String fieldName) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(fieldName + " required");
            }
        }
    }

    /**
     * Creates a registry backed by the given configuration and secret lookups.
     *
     * @param configLookup   resolves model names to configuration descriptors
     * @param secretResolver resolves API key secret references
     */
    public InferenceProviderRegistry(Function<String, InferenceModelConfig> configLookup,
                                     SecretResolver secretResolver) {
        if (configLookup == null) {
            throw new IllegalArgumentException("configLookup required");
        }
        if (secretResolver == null) {
            throw new IllegalArgumentException("secretResolver required");
        }
        this.configLookup = configLookup;
        this.secretResolver = secretResolver;
    }

    /**
     * Resolves the inference provider for the given model name.
     *
     * @param modelName the registered model name
     * @return the provider instance, or null if not configured
     */
    public InferenceProvider resolve(String modelName) {
        if (modelName == null || modelName.isBlank()) {
            return null;
        }
        return providers.computeIfAbsent(modelName, this::createProvider);
    }

    /**
     * Invalidates the cached provider for the given model name.
     */
    public void invalidate(String modelName) {
        if (modelName != null) {
            providers.remove(modelName);
        }
    }

    private InferenceProvider createProvider(String modelName) {
        InferenceModelConfig config = configLookup.apply(modelName);
        if (config == null) {
            throw new IllegalStateException(
                    "No inference model config for: " + modelName);
        }
        String apiKey = secretResolver.resolve(config.apiKeySecret());
        return new OpenAiCompatibleInferenceProvider(
                config.endpoint(), config.name(), apiKey,
                config.maxBatchSize(), config.maxRequestsPerSecond(),
                config.timeoutSeconds());
    }
}
