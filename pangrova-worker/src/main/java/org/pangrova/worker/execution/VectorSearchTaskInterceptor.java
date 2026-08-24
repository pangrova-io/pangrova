/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.worker.execution;

import org.pangrova.protocol.v1.VectorSearchOperator;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorIndexLoader;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.spi.observability.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Intercepts task requests carrying vector search metadata in session properties
 * and delegates execution to {@link VectorSearchExecutor}.
 *
 * <p>The coordinator serializes a {@link VectorSearchOperator} proto as Base64
 * in session property {@code pangrova.vector_search_operator}. This interceptor
 * detects the property, deserializes the operator, and executes ANN search
 * on each split's data file.</p>
 *
 * <p>Lazily discovers {@link VectorIndexLoader} via {@link ServiceLoader} on
 * first use. If no implementation is on the classpath, vector search tasks
 * fall through to the standard native execution path.</p>
 *
 * <p>Thread-safe: double-checked locking for lazy initialization.</p>
 */
public final class VectorSearchTaskInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(VectorSearchTaskInterceptor.class);

    static final String SESSION_PROPERTY_KEY = "pangrova.vector_search_operator";
    private static final long PER_FILE_TIMEOUT_SECONDS = 30;
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    private static volatile VectorSearchExecutor sharedExecutor;
    private static volatile boolean initialized;
    private static final ExecutorService SEARCH_POOL = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            r -> {
                Thread t = new Thread(r, "vector-search-file-" + THREAD_COUNTER.getAndIncrement());
                t.setDaemon(true);
                return t;
            });

    private VectorSearchTaskInterceptor() {}

    /**
     * Returns true if the session properties contain a vector search operator.
     */
    public static boolean isVectorSearchTask(Map<String, String> sessionProperties) {
        return sessionProperties != null && sessionProperties.containsKey(SESSION_PROPERTY_KEY);
    }

    /**
     * Attempts to execute vector search. Returns null if no VectorIndexLoader
     * is available on the classpath (caller should fall through to native path).
     *
     * @param sessionProperties session properties containing the serialized operator
     * @param splitFilePaths    data file paths extracted from the task's splits
     * @return merged search result, or null if vector search SPI is unavailable
     */
    public static VectorSearchResult tryExecute(Map<String, String> sessionProperties,
                                                 List<String> splitFilePaths) {
        VectorSearchExecutor executor = resolveExecutor();
        if (executor == null) {
            LOG.debug("event=vector_search_skip reason=no_vector_index_loader");
            return null;
        }
        return doExecute(executor, sessionProperties, splitFilePaths);
    }

    private static VectorSearchResult doExecute(VectorSearchExecutor executor,
                                                 Map<String, String> sessionProperties,
                                                 List<String> splitFilePaths) {
        VectorSearchOperator operator = deserializeOperator(sessionProperties);
        float[] queryVector = toFloatArray(operator.getQueryVector().getValuesList());
        DistanceMetric metric = DistanceMetric.valueOf(operator.getDistanceMetric());
        int topK = operator.getTopK();

        LOG.info("event=vector_search_execute table={} column={} topK={} metric={} indexId={} files={}",
                operator.getTableName(), operator.getVectorColumn(),
                topK, metric, operator.getIndexId(), splitFilePaths.size());

        List<CompletableFuture<VectorSearchResult>> tasks = new ArrayList<>(splitFilePaths.size());
        List<CompletableFuture<VectorSearchResult>> withTimeouts = new ArrayList<>(splitFilePaths.size());
        for (String filePath : splitFilePaths) {
            CompletableFuture<VectorSearchResult> task = CompletableFuture
                    .supplyAsync(() -> executor.execute(
                            filePath, operator.getVectorColumn(), queryVector,
                            topK, metric, operator.getSearchParamsMap()), SEARCH_POOL);
            tasks.add(task);
            withTimeouts.add(task.orTimeout(PER_FILE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        return collectAndMerge(tasks, withTimeouts, splitFilePaths, operator, topK);
    }

    /** Aggregate outcome of collecting per-file partial vector search results. */
    private record CollectionOutcome(
            List<VectorSearchResult> partialResults, int failureCount, int unindexedCount) {}

    private static VectorSearchResult collectAndMerge(
            List<CompletableFuture<VectorSearchResult>> tasks,
            List<CompletableFuture<VectorSearchResult>> withTimeouts,
            List<String> splitFilePaths,
            VectorSearchOperator operator,
            int topK) {
        CollectionOutcome outcome = collectPartialResults(tasks, withTimeouts, splitFilePaths);
        int totalFiles = splitFilePaths.size();
        if (outcome.unindexedCount() > 0) {
            LOG.warn("event=vector_search_unindexed_files table={} column={} "
                    + "unindexedFiles={} totalFiles={} "
                    + "hint=create_vector_index_for_full_coverage",
                    operator.getTableName(), operator.getVectorColumn(),
                    outcome.unindexedCount(), totalFiles);
        }
        if (outcome.failureCount() > 0 && outcome.failureCount() * 2 > totalFiles) {
            LOG.warn("event=vector_search_majority_failed table={} column={} "
                    + "failedFiles={} totalFiles={} action=fallback_to_native",
                    operator.getTableName(), operator.getVectorColumn(),
                    outcome.failureCount(), totalFiles);
            return null;
        }
        if (outcome.partialResults().isEmpty() && !splitFilePaths.isEmpty()) {
            LOG.info("event=vector_search_no_index_results table={} column={} files={} "
                    + "action=fallback_to_native",
                    operator.getTableName(), operator.getVectorColumn(), splitFilePaths.size());
            return null;
        }
        VectorSearchResult merged = mergeResults(outcome.partialResults(), topK);
        if (merged.neighbors().isEmpty()) {
            LOG.info("event=vector_search_empty_results table={} column={} files={} "
                    + "action=fallback_to_native",
                    operator.getTableName(), operator.getVectorColumn(), splitFilePaths.size());
            return null;
        }
        return merged;
    }

    private static CollectionOutcome collectPartialResults(
            List<CompletableFuture<VectorSearchResult>> tasks,
            List<CompletableFuture<VectorSearchResult>> withTimeouts,
            List<String> splitFilePaths) {
        List<VectorSearchResult> partialResults = new ArrayList<>(withTimeouts.size());
        int failureCount = 0;
        int unindexedCount = 0;
        for (int i = 0; i < withTimeouts.size(); i++) {
            try {
                VectorSearchResult result = withTimeouts.get(i).join();
                if (!result.indexFound()) {
                    unindexedCount++;
                    LOG.info("event=vector_search_no_index file={} action=needs_native_brute_force",
                            splitFilePaths.get(i));
                }
                partialResults.add(result);
            } catch (java.util.concurrent.CompletionException e) {
                failureCount++;
                handleFileFailure(tasks.get(i), splitFilePaths.get(i), e.getCause());
            }
        }
        return new CollectionOutcome(partialResults, failureCount, unindexedCount);
    }

    private static void handleFileFailure(CompletableFuture<VectorSearchResult> task,
                                          String filePath, Throwable cause) {
        if (cause instanceof TimeoutException) {
            task.cancel(true);
            LOG.warn("event=vector_search_file_timeout file={} timeoutSeconds={}",
                    filePath, PER_FILE_TIMEOUT_SECONDS);
        } else {
            LOG.warn("event=vector_search_file_failed file={} error={}",
                    filePath, cause.getMessage());
        }
    }

    private static VectorSearchExecutor resolveExecutor() {
        if (!initialized) {
            synchronized (VectorSearchTaskInterceptor.class) {
                if (!initialized) {
                    sharedExecutor = tryCreateExecutor();
                    initialized = true;
                }
            }
        }
        return sharedExecutor;
    }

    private static VectorSearchExecutor tryCreateExecutor() {
        try {
            ServiceLoader<VectorIndexLoader> loader = ServiceLoader.load(VectorIndexLoader.class);
            for (VectorIndexLoader indexLoader : loader) {
                LOG.info("event=vector_search_executor_initialized loader={}",
                        indexLoader.getClass().getName());
                RetrievalQualitySampler sampler = tryCreateSampler(indexLoader);
                return new VectorSearchExecutor(indexLoader, sampler);
            }
        } catch (Exception e) {
            LOG.debug("event=vector_search_executor_init_failed error={}", e.getMessage());
        }
        return null;
    }

    private static RetrievalQualitySampler tryCreateSampler(VectorIndexLoader indexLoader) {
        try {
            ServiceLoader<MetricsProvider> metricsLoader = ServiceLoader.load(MetricsProvider.class);
            for (MetricsProvider metricsProvider : metricsLoader) {
                LOG.debug("event=retrieval_quality_sampler_initialized");
                return new RetrievalQualitySampler(indexLoader, metricsProvider,
                        RetrievalQualitySampler.DEFAULT_SAMPLING_RATE);
            }
        } catch (Exception e) {
            LOG.debug("event=retrieval_quality_sampler_init_failed error={}", e.getMessage());
        }
        return null;
    }

    private static VectorSearchOperator deserializeOperator(Map<String, String> props) {
        String encoded = props.get(SESSION_PROPERTY_KEY);
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            return VectorSearchOperator.parseFrom(bytes);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to deserialize VectorSearchOperator from session property", e);
        }
    }

    private static float[] toFloatArray(List<Float> values) {
        float[] result = new float[values.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = values.get(i);
        }
        return result;
    }

    private static VectorSearchResult mergeResults(List<VectorSearchResult> partials,
                                                    int topK) {
        PriorityQueue<VectorSearchResult.ScoredRow> heap = new PriorityQueue<>(
                Math.max(topK, 1),
                Comparator.comparingDouble(VectorSearchResult.ScoredRow::distance).reversed());
        for (VectorSearchResult partial : partials) {
            for (VectorSearchResult.ScoredRow row : partial.neighbors()) {
                heap.offer(row);
                if (heap.size() > topK) {
                    heap.poll();
                }
            }
        }
        List<VectorSearchResult.ScoredRow> merged = new ArrayList<>(heap);
        merged.sort(Comparator.comparingDouble(VectorSearchResult.ScoredRow::distance));
        return new VectorSearchResult("global", merged);
    }
}
