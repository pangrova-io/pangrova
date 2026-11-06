package org.pangrova.ai.worker.embedding;

import org.pangrova.ai.spi.embedding.EmbeddingProvider;

/**
 * Resolves a registered model name to its corresponding embedding provider instance.
 *
 * <p>Used by the worker to obtain the provider for a given model during DML execution.
 * Implementations typically delegate to the coordinator's model registry via RPC
 * or a local cache of provider instances.</p>
 */
public interface EmbeddingProviderLookup {

    /**
     * Resolves the embedding provider for the given model name.
     *
     * @param modelName the registered model name (must not be null or blank)
     * @return the provider instance, or null if no provider is available for the model
     */
    EmbeddingProvider resolve(String modelName);
}
