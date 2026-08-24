package org.pangrova.worker.ai.embedding;

import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.ai.embedding.SecretResolver;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EmbeddingBootstrap wiring (Task 194).
 */
class EmbeddingBootstrapTest {

    @Test
    void startAndStopLifecycle() {
        EmbeddingQueue stubQueue = new StubEmbeddingQueue();
        SecretResolver resolver = ref -> "test-key";
        Function<String, EmbeddingModelDescriptor> descriptorLookup = name ->
                new EmbeddingModelDescriptor(
                        name, "openai-compatible", "http://localhost:8080/v1/embeddings",
                        384, "env://KEY", 32, 10, "", "", 0);

        var bootstrap = new EmbeddingBootstrap(descriptorLookup, resolver, stubQueue);

        assertFalse(bootstrap.isStarted());
        bootstrap.start();
        assertTrue(bootstrap.isStarted());
        assertNotNull(bootstrap.syncExecutor());
        assertNotNull(bootstrap.providerRegistry());

        bootstrap.close();
        assertFalse(bootstrap.isStarted());
    }

    @Test
    void doubleStartIsIdempotent() {
        EmbeddingQueue stubQueue = new StubEmbeddingQueue();
        SecretResolver resolver = ref -> "test-key";
        Function<String, EmbeddingModelDescriptor> descriptorLookup = name -> null;

        var bootstrap = new EmbeddingBootstrap(descriptorLookup, resolver, stubQueue);
        bootstrap.start();
        bootstrap.start(); // should not throw
        assertTrue(bootstrap.isStarted());
        bootstrap.close();
    }

    @Test
    void nullDependenciesThrow() {
        EmbeddingQueue stubQueue = new StubEmbeddingQueue();
        SecretResolver resolver = ref -> "test-key";

        assertThrows(IllegalArgumentException.class, () ->
                new EmbeddingBootstrap(null, resolver, stubQueue));
        assertThrows(IllegalArgumentException.class, () ->
                new EmbeddingBootstrap(name -> null, null, stubQueue));
        assertThrows(IllegalArgumentException.class, () ->
                new EmbeddingBootstrap(name -> null, resolver, null));
    }

    private static class StubEmbeddingQueue implements EmbeddingQueue {
        @Override
        public List<PendingEmbeddingRow> poll(int maxBatchSize) {
            return List.of();
        }

        @Override
        public void commit(List<EmbeddingResult> results) {}

        @Override
        public void reEnqueue(List<PendingEmbeddingRow> rows) {}

        @Override
        public void markFailed(List<PendingEmbeddingRow> rows) {}
    }
}
