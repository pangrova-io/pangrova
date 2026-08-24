package org.pangrova.worker.ai;

import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.worker.ai.embedding.EmbeddingProviderLookup;
import org.pangrova.worker.ai.inference.InferenceProviderRegistry;
import org.pangrova.worker.ai.rerank.WorkerRerankerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unified model service abstraction on the Worker data plane.
 *
 * <p>Aggregates all AI capabilities (embedding, inference, reranking) behind
 * a single service facade. The Coordinator only plans the AI endpoint and
 * returns a Flight ticket; the Worker executes all model operations locally
 * via this service.</p>
 *
 * <p>This moves model serving to the data plane, where it belongs:
 * <ul>
 *   <li>Embedding: {@link EmbeddingProviderLookup} for sync/async vector generation</li>
 *   <li>Inference: {@link InferenceProviderRegistry} for LLM completions</li>
 *   <li>Reranking: {@link WorkerRerankerExecutor} for result fusion and scoring</li>
 * </ul>
 *
 * <p>Thread-safe: all constituent providers are thread-safe per their SPI contracts.</p>
 */
public final class ModelServiceProvider implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ModelServiceProvider.class);

    private final EmbeddingProviderLookup embeddingLookup;
    private final InferenceProviderRegistry inferenceLookup;
    private final WorkerRerankerExecutor rerankerExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ModelServiceProvider(
            EmbeddingProviderLookup embeddingLookup,
            InferenceProviderRegistry inferenceLookup,
            WorkerRerankerExecutor rerankerExecutor) {
        this.embeddingLookup = Objects.requireNonNull(embeddingLookup, "embeddingLookup required");
        this.inferenceLookup = Objects.requireNonNull(inferenceLookup, "inferenceLookup required");
        this.rerankerExecutor = Objects.requireNonNull(rerankerExecutor, "rerankerExecutor required");
    }

    /**
     * Returns the embedding provider for the given model name.
     *
     * @param modelName the registered model name
     * @return the embedding provider, or null if not found
     */
    public EmbeddingProvider getEmbeddingProvider(String modelName) {
        checkNotClosed();
        return embeddingLookup.resolve(modelName);
    }

    /**
     * Returns the inference provider for the given model name.
     *
     * @param modelName the registered model name
     * @return the inference provider, or null if not found
     */
    public InferenceProvider getInferenceProvider(String modelName) {
        checkNotClosed();
        return inferenceLookup.resolve(modelName);
    }

    /**
     * Returns the reranker executor for hybrid retrieval operations.
     */
    public WorkerRerankerExecutor getRerankerExecutor() {
        checkNotClosed();
        return rerankerExecutor;
    }

    /**
     * Returns whether any model service capability is available.
     */
    public boolean isAvailable() {
        return !closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            LOG.info("event=model_service_provider_closed");
        }
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("ModelServiceProvider has been closed");
        }
    }
}
