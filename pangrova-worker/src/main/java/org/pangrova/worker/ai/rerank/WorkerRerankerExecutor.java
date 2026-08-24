package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.RerankerFactory;
import org.pangrova.spi.ai.rerank.ScoredCandidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker-side executor for reranking operations.
 *
 * <p>Discovers {@link RerankerFactory} implementations via ServiceLoader,
 * creates reranker instances on demand, and executes reranking on candidate
 * rows. This wires the RerankerInjectionRule plan nodes to actual execution
 * on the Worker data plane.</p>
 *
 * <p>Supports all reranker strategies: RRF, weighted linear, cross-encoder,
 * and cascade (multi-stage). The executor caches factory instances for reuse.</p>
 *
 * <p>Thread-safe: factories are loaded once, reranker instances are stateless.</p>
 */
public final class WorkerRerankerExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerRerankerExecutor.class);

    private static final long RELOAD_INTERVAL_MINUTES = 30;

    private volatile ConcurrentHashMap<String, RerankerFactory> factoryCache = new ConcurrentHashMap<>();
    private volatile boolean initialized = false;
    private final ScheduledExecutorService reloadScheduler;

    /**
     * Creates an executor and starts the periodic reranker-factory reload task.
     */
    public WorkerRerankerExecutor() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "reranker-factory-reload");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::reload,
                RELOAD_INTERVAL_MINUTES, RELOAD_INTERVAL_MINUTES, TimeUnit.MINUTES);
        this.reloadScheduler = scheduler;
    }

    /**
     * Executes reranking with the specified strategy and configuration.
     *
     * @param strategyName reranker strategy name (rrf, weighted, cross_encoder, cascade)
     * @param properties   strategy-specific configuration
     * @param query        the original query text for relevance scoring
     * @param candidates   scored candidates from retrieval sources
     * @param topK         maximum results to return
     * @return ranked results sorted by descending fused score
     */
    public List<RankedResult> execute(
            String strategyName,
            Map<String, String> properties,
            String query,
            List<ScoredCandidate> candidates,
            int topK) {

        Objects.requireNonNull(strategyName, "strategyName required");
        Objects.requireNonNull(query, "query required");
        Objects.requireNonNull(candidates, "candidates required");
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }

        ensureInitialized();

        RerankerFactory factory = factoryCache.get(strategyName);
        if (factory == null) {
            throw new IllegalStateException(
                    "No reranker factory found for strategy: " + strategyName
                            + ". Available: " + factoryCache.keySet());
        }

        Reranker reranker = factory.create(properties != null ? properties : Map.of());

        LOG.info("event=worker_rerank_execute strategy={} candidates={} topK={}",
                new Object[]{strategyName, candidates.size(), topK});

        List<RankedResult> results = reranker.rerank(query, candidates, topK);

        LOG.info("event=worker_rerank_completed strategy={} results={}",
                strategyName, results.size());

        return results;
    }

    /**
     * Returns whether a reranker factory is available for the given strategy.
     */
    public boolean hasStrategy(String strategyName) {
        ensureInitialized();
        return factoryCache.containsKey(strategyName);
    }

    /**
     * Returns the set of available reranker strategy names.
     */
    public java.util.Set<String> availableStrategies() {
        ensureInitialized();
        return java.util.Collections.unmodifiableSet(factoryCache.keySet());
    }

    /**
     * Re-discovers reranker factories via ServiceLoader, enabling hot deployment
     * of new reranker plugins without restarting the worker.
     */
    public synchronized void reload() {
        factoryCache = discoverFactories();
        LOG.info("event=reranker_factories_reloaded count={} strategies={}",
                factoryCache.size(), factoryCache.keySet());
    }

    public void close() {
        reloadScheduler.shutdownNow();
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    factoryCache = discoverFactories();
                    LOG.info("event=reranker_factories_initialized count={} strategies={}",
                            factoryCache.size(), factoryCache.keySet());
                    initialized = true;
                }
            }
        }
    }

    /**
     * Single discovery path: ServiceLoader + built-in fallbacks.
     * Eliminates duplication between initialization and reload.
     */
    private static ConcurrentHashMap<String, RerankerFactory> discoverFactories() {
        ConcurrentHashMap<String, RerankerFactory> cache = new ConcurrentHashMap<>();
        ServiceLoader<RerankerFactory> loader = ServiceLoader.load(RerankerFactory.class);
        for (RerankerFactory factory : loader) {
            cache.put(factory.name(), factory);
            LOG.info("event=reranker_factory_loaded name={} class={}",
                    factory.name(), factory.getClass().getName());
        }
        // Register built-in factories if not loaded via ServiceLoader
        cache.computeIfAbsent("rrf", name -> new RrfRerankerFactory());
        cache.computeIfAbsent("weighted", name -> new WeightedRerankerFactory());
        cache.computeIfAbsent("cross_encoder", name -> new CrossEncoderRerankerFactory());
        cache.computeIfAbsent("cascade", name -> new CascadeRerankerFactory());
        return cache;
    }
}
