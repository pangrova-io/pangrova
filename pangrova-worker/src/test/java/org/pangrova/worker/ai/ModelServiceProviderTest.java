package org.pangrova.worker.ai;

import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.worker.ai.embedding.EmbeddingProviderLookup;
import org.pangrova.worker.ai.inference.InferenceProviderRegistry;
import org.pangrova.worker.ai.rerank.WorkerRerankerExecutor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ModelServiceProvider (Task 197).
 */
class ModelServiceProviderTest {

    @Test
    void providerAggregatesAllCapabilities() {
        EmbeddingProviderLookup embeddingLookup = name -> null;
        InferenceProviderRegistry inferenceLookup =
                new InferenceProviderRegistry(name -> null, ref -> "key");
        WorkerRerankerExecutor reranker = new WorkerRerankerExecutor();

        var provider = new ModelServiceProvider(embeddingLookup, inferenceLookup, reranker);

        assertTrue(provider.isAvailable());
        assertNull(provider.getEmbeddingProvider("nonexistent"));
        assertNotNull(provider.getRerankerExecutor());
    }

    @Test
    void closedProviderThrows() {
        EmbeddingProviderLookup embeddingLookup = name -> null;
        InferenceProviderRegistry inferenceLookup =
                new InferenceProviderRegistry(name -> null, ref -> "key");
        WorkerRerankerExecutor reranker = new WorkerRerankerExecutor();

        var provider = new ModelServiceProvider(embeddingLookup, inferenceLookup, reranker);
        provider.close();

        assertFalse(provider.isAvailable());
        assertThrows(IllegalStateException.class,
                () -> provider.getEmbeddingProvider("test"));
    }

    @Test
    void nullDependenciesThrow() {
        assertThrows(NullPointerException.class,
                () -> new ModelServiceProvider(null, null, null));
    }
}
