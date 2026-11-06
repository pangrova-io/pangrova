package org.pangrova.indexing.eval;

import org.pangrova.indexing.registry.IndexEngineRegistry;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexFileReadResult;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.connector.spi.index.FileSystemAccessor;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.connector.api.index.IndexEvalResult;
import org.pangrova.indexing.spi.strategy.IndexStrategyProvider;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.indexing.api.evaluation.IndexEvalDescriptor;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Evaluates index predicates against data files using cached or stored index data.
 *
 * <p>Multi-hint aggregation: any SKIP → SKIP (short-circuit),
 * any KEEP + no SKIP → KEEP, all UNKNOWN → UNKNOWN.</p>
 */
public final class IndexEvaluationService {

    private static final Logger LOG = LoggerFactory.getLogger(IndexEvaluationService.class);

    private final IndexEngineRegistry engineRegistry;
    private final IndexCache cache;
    private final IndexStorageManager storageManager;
    private final IndexSerializer serializer;
    private final IndexEvalCircuitBreaker circuitBreaker;

    /** Backward-compatible constructor without circuit breaker. */
    public IndexEvaluationService(
            IndexEngineRegistry engineRegistry,
            IndexCache cache,
            IndexStorageManager storageManager,
            IndexSerializer serializer) {
        this(engineRegistry, cache, storageManager, serializer, null);
    }

    /**
     * Creates an evaluation service with optional circuit breaker.
     *
     * @param circuitBreaker per-index circuit breaker (nullable — disabled when null)
     */
    public IndexEvaluationService(
            IndexEngineRegistry engineRegistry,
            IndexCache cache,
            IndexStorageManager storageManager,
            IndexSerializer serializer,
            IndexEvalCircuitBreaker circuitBreaker) {
        this.engineRegistry = engineRegistry;
        this.cache = cache;
        this.storageManager = storageManager;
        this.serializer = serializer;
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Evaluates a single data file against a single index and predicate.
     *
     * <p>When a circuit breaker is configured and OPEN for this index,
     * evaluation is skipped and UNKNOWN is returned immediately.</p>
     */
    public IndexEvalResult evaluateFile(
            IndexDescriptor descriptor,
            String dataFilePath,
            String indexFilePath,
            IndexPredicate predicate,
            FileSystemAccessor fsAccessor) {
        return evaluateFileInternal(
                descriptor.indexId(), descriptor.indexType(), descriptor.generation(),
                dataFilePath, indexFilePath, predicate, fsAccessor);
    }

    /**
     * Worker-side overload: evaluates with only the minimum metadata available
     * in a {@link IndexEvalHint}, with no placeholder fields synthesized.
     */
    public IndexEvalResult evaluateFile(
            IndexEvalDescriptor descriptor,
            String dataFilePath,
            String indexFilePath,
            IndexPredicate predicate,
            FileSystemAccessor fsAccessor) {
        return evaluateFileInternal(
                descriptor.indexId(), descriptor.indexType(), descriptor.generation(),
                dataFilePath, indexFilePath, predicate, fsAccessor);
    }

    private IndexEvalResult evaluateFileInternal(
            String indexId, IndexType indexType, int generation,
            String dataFilePath, String indexFilePath,
            IndexPredicate predicate, FileSystemAccessor fsAccessor) {

        IndexEvaluationFailures.checkInterrupted();
        if (!breakerAllows(indexId)) {
            LOG.debug("action=circuit_breaker_open indexId={} file={}", indexId, dataFilePath);
            return IndexEvalResult.UNKNOWN;
        }

        LoadOutcome outcome = loadIndexData(
                indexId, indexType, generation, dataFilePath, indexFilePath, fsAccessor);
        IndexEvaluationFailures.checkInterrupted();
        if (outcome.data() == null) {
            // Only a genuine read/deserialize ERROR trips the breaker. A MISSING
            // index is normal during the write -> asynchronous-build window and
            // must NOT count as a failure (finding F-INDEX-03); otherwise the
            // breaker opens during healthy operation and silently disables
            // index-based file skipping.
            if (outcome.failure()) {
                recordBreakerFailure(indexId);
            }
            return IndexEvalResult.UNKNOWN;
        }

        try {
            IndexStrategyProvider engine = engineRegistry.getProvider(indexType);
            IndexEvalResult result = engine.evaluate(outcome.data(), predicate);
            IndexEvaluationFailures.checkInterrupted();
            recordBreakerSuccess(indexId);
            return result;
        } catch (Exception e) {
            IndexEvaluationFailures.propagateIfTerminal(e);
            LOG.warn("action=eval_failed indexId={} file={} error={}",
                    indexId, dataFilePath, e.getMessage());
            recordBreakerFailure(indexId);
            return IndexEvalResult.UNKNOWN;
        }
    }

    private boolean breakerAllows(String indexId) {
        return circuitBreaker == null || circuitBreaker.isAllowed(indexId);
    }

    private void recordBreakerSuccess(String indexId) {
        if (circuitBreaker != null) {
            circuitBreaker.recordSuccess(indexId);
        }
    }

    private void recordBreakerFailure(String indexId) {
        if (circuitBreaker != null) {
            circuitBreaker.recordFailure(indexId);
        }
    }

    /**
     * Aggregates evaluation results across multiple index hints for a single data file.
     *
     * <p>Aggregation rules:
     * <ul>
     *   <li>Any SKIP → SKIP (short-circuit)</li>
     *   <li>Any KEEP + no SKIP → KEEP</li>
     *   <li>All UNKNOWN → UNKNOWN</li>
     * </ul>
     *
     * <p>Ordinary loading failure for a hint is UNKNOWN. Authority failures,
     * cancellation and interruption abort evaluation instead.
     */
    public IndexEvalResult aggregateResults(
            List<IndexDescriptor> descriptors,
            String dataFilePath,
            IndexPredicate predicate,
            FileSystemAccessor fsAccessor,
            java.util.function.Function<String, String> indexFilePathResolver) {

        boolean hasKeep = false;

        for (IndexDescriptor descriptor : descriptors) {
            IndexEvaluationFailures.checkInterrupted();
            try {
                String indexFilePath = indexFilePathResolver.apply(descriptor.indexId());
                if (indexFilePath == null) {
                    continue;
                }
                IndexEvalResult result = evaluateFile(
                        descriptor, dataFilePath, indexFilePath, predicate, fsAccessor);
                if (result == IndexEvalResult.SKIP) {
                    return IndexEvalResult.SKIP;
                }
                if (result == IndexEvalResult.KEEP) {
                    hasKeep = true;
                }
            } catch (Exception e) {
                IndexEvaluationFailures.propagateIfTerminal(e);
                LOG.warn("Failed to evaluate index={} for file={}, treating as UNKNOWN",
                        descriptor.indexId(), dataFilePath, e);
            }
        }

        return hasKeep ? IndexEvalResult.KEEP : IndexEvalResult.UNKNOWN;
    }

    /**
     * Loads index data from cache or storage, populating cache on miss.
     *
     * <p>Distinguishes a normal missing index from a genuine failure so the
     * caller can decide whether to trip the circuit breaker. A missing index or
     * an unsupported (future) format version is neutral; only I/O errors and
     * deserialization failures are reported as failures.</p>
     */
    private LoadOutcome loadIndexData(
            String indexId,
            IndexType expectedType,
            int generation,
            String dataFilePath,
            String indexFilePath,
            FileSystemAccessor fsAccessor) {

        IndexData cached = cache.get(indexId, dataFilePath, generation);
        if (cached != null) {
            return LoadOutcome.present(cached);
        }

        IndexFileReadResult read = storageManager.readIndexResult(fsAccessor, indexFilePath);
        if (read.isMissing()) {
            LOG.debug("action=index_missing indexId={} file={}", indexId, indexFilePath);
            return LoadOutcome.MISSING;
        }
        if (read.isError()) {
            LOG.debug("action=index_read_error indexId={} file={}", indexId, indexFilePath);
            return LoadOutcome.FAILED;
        }

        try {
            Optional<IndexData> deserialized = serializer.deserialize(read.bytes());
            if (deserialized.isEmpty()) {
                LOG.debug("action=skip_unsupported_version indexId={} file={}", indexId, indexFilePath);
                return LoadOutcome.MISSING; // version skip is not a failure
            }
            IndexData data = deserialized.get();
            if (data.indexType() != expectedType) {
                throw new IllegalArgumentException("Index payload type differs from descriptor: " + expectedType);
            }
            IndexData prepared = engineRegistry.getProvider(expectedType).prepareForEvaluation(data);
            if (prepared == null || prepared.indexType() != expectedType) {
                throw new IllegalArgumentException("Provider prepared incompatible index data: " + expectedType);
            }
            IndexEvaluationFailures.checkInterrupted();
            cache.put(indexId, dataFilePath, generation, prepared);
            return LoadOutcome.present(prepared);
        } catch (Exception e) {
            IndexEvaluationFailures.propagateIfTerminal(e);
            LOG.warn("action=index_deserialize_failed indexId={} file={}", indexId, indexFilePath, e);
            return LoadOutcome.FAILED; // corrupt data is a genuine error
        }
    }

    /**
     * Result of a load attempt: either present data, or absent with a flag that
     * separates a genuine failure (I/O error, corrupt data) from a normal miss
     * (no index yet, unsupported version) for circuit-breaker accounting.
     */
    private record LoadOutcome(IndexData data, boolean failure) {
        static final LoadOutcome MISSING = new LoadOutcome(null, false);
        static final LoadOutcome FAILED = new LoadOutcome(null, true);

        static LoadOutcome present(IndexData data) {
            return new LoadOutcome(data, false);
        }
    }
}
