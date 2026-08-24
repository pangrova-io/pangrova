package org.pangrova.worker.ai.embedding.provider;

import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;

/**
 * Creates {@link OpenAiCompatibleEmbeddingProvider} instances from model descriptors.
 *
 * <p>Resolves the API key from the descriptor's secret reference and constructs
 * a fully configured provider ready for concurrent use.</p>
 */
public final class OpenAiCompatibleProviderFactory {

    private static final String PROVIDER_TYPE = "openai-compatible";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    /**
     * Returns whether this factory handles the given provider type.
     *
     * @param providerType the provider type identifier from the model descriptor
     * @return true if this factory can create a provider for the given type
     */
    public boolean supports(String providerType) {
        return PROVIDER_TYPE.equals(providerType);
    }

    /**
     * Creates an embedding provider from the given descriptor and resolved API key.
     *
     * @param descriptor the model descriptor containing endpoint, dimensions, etc.
     * @param resolvedApiKey the actual API key (already resolved from secret reference)
     * @return a configured, thread-safe embedding provider
     * @throws IllegalArgumentException if descriptor or resolvedApiKey is null
     */
    public EmbeddingProvider create(EmbeddingModelDescriptor descriptor, String resolvedApiKey) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor required");
        }
        if (resolvedApiKey == null || resolvedApiKey.isBlank()) {
            throw new IllegalArgumentException("resolvedApiKey required");
        }

        return new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                descriptor.endpoint(),
                descriptor.name(),
                resolvedApiKey,
                descriptor.dimension(),
                descriptor.maxBatchSize(),
                descriptor.maxRequestsPerSecond(),
                descriptor.queryPrefix(),
                descriptor.documentPrefix(),
                DEFAULT_TIMEOUT_SECONDS));
    }
}
