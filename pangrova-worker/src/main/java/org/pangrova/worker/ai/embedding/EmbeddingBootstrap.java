package org.pangrova.worker.ai.embedding;

import org.pangrova.spi.ai.embedding.SecretResolver;
import org.pangrova.worker.ai.embedding.provider.EmbeddingProviderRegistry;
import org.pangrova.worker.ai.embedding.provider.OpenAiCompatibleProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Bootstraps the Worker-side embedding infrastructure for production use.
 *
 * <p>Wires the {@link EmbeddingProviderRegistry} with model descriptor lookup
 * and secret resolution, starts the {@link AsyncEmbeddingDrainer}, and exposes
 * the {@link SyncEmbeddingExecutor} for DML-path use.</p>
 *
 * <p>Call {@link #start()} after worker resource initialization.
 * Call {@link #close()} during shutdown (reverse order).</p>
 */
public final class EmbeddingBootstrap implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddingBootstrap.class);

    private final EmbeddingProviderRegistry providerRegistry;
    private final SyncEmbeddingExecutor syncExecutor;
    private final AsyncEmbeddingDrainer drainer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * Creates the embedding bootstrap with the given dependencies.
     *
     * @param descriptorLookup resolves model names to descriptors (typically from coordinator RPC)
     * @param secretResolver   resolves API key secret references
     * @param queue            the embedding queue (may be backed by persistent store)
     */
    public EmbeddingBootstrap(
            Function<String, org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor> descriptorLookup,
            SecretResolver secretResolver,
            EmbeddingQueue queue) {
        if (descriptorLookup == null) {
            throw new IllegalArgumentException("descriptorLookup required");
        }
        if (secretResolver == null) {
            throw new IllegalArgumentException("secretResolver required");
        }
        if (queue == null) {
            throw new IllegalArgumentException("queue required");
        }

        var factory = new OpenAiCompatibleProviderFactory();
        this.providerRegistry = new EmbeddingProviderRegistry(
                descriptorLookup, secretResolver, factory);
        this.syncExecutor = new SyncEmbeddingExecutor(providerRegistry);
        this.drainer = new AsyncEmbeddingDrainer(queue, providerRegistry);
    }

    /**
     * Starts the async embedding drainer background thread.
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            drainer.start();
            LOG.info("event=embedding_bootstrap_started");
        }
    }

    @Override
    public void close() {
        if (started.compareAndSet(true, false)) {
            drainer.close();
            LOG.info("event=embedding_bootstrap_stopped");
        }
    }

    /**
     * Returns the sync executor for DML-path embedding computation.
     */
    public SyncEmbeddingExecutor syncExecutor() {
        return syncExecutor;
    }

    /**
     * Returns the provider registry for model lookup.
     */
    public EmbeddingProviderRegistry providerRegistry() {
        return providerRegistry;
    }

    /**
     * Returns whether the bootstrap has been started.
     */
    public boolean isStarted() {
        return started.get();
    }
}
