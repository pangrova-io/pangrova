package org.pangrova.worker.task;

import org.pangrova.spi.ai.rerank.RankedResult;
import org.pangrova.spi.ai.rerank.ScoredCandidate;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.worker.ai.rerank.WorkerRerankerExecutor;
import org.pangrova.worker.execution.VectorSearchResultBuffer;
import org.pangrova.worker.execution.VectorSearchTaskInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Vector-search task execution support, extracted from {@link TaskLifecycleManager}
 * to isolate the Java-side ANN index path from the standard native lifecycle flow.
 *
 * <p>Handles detection of vector-search tasks via session properties, execution via
 * {@link VectorSearchTaskInterceptor}, optional reranking, result serialization with
 * DEFLATE compression, and buffering for coordinator retrieval.</p>
 *
 * <p>Concurrency strategy: stateless beyond the thread-safe result buffer; the
 * reranker executor and status collector are themselves thread-safe.</p>
 */
final class VectorSearchTaskSupport {

    private static final Logger LOG = LoggerFactory.getLogger(VectorSearchTaskSupport.class);

    /** DEFLATE compression magic header ("AEZC") for auto-detection on coordinator side. */
    private static final int COMPRESSION_MAGIC = 0x41455A43;

    /** Bit shift extracting the most-significant byte of a 32-bit big-endian value. */
    private static final int HIGH_BYTE_SHIFT = 24;

    private final VectorSearchResultBuffer vectorResultBuffer;
    private final WorkerRerankerExecutor rerankerExecutor;
    private final TaskStatusCollector statusCollector;
    private final Consumer<TaskInfo> memoryReleaser;

    /**
     * Creates the support with its collaborators.
     *
     * @param vectorResultBuffer buffer holding serialized results for coordinator retrieval
     * @param rerankerExecutor   executes configured reranker strategies
     * @param statusCollector    status reporter (may be null in minimal configurations)
     * @param memoryReleaser     releases the task memory budget after completion
     */
    VectorSearchTaskSupport(VectorSearchResultBuffer vectorResultBuffer,
                            WorkerRerankerExecutor rerankerExecutor,
                            TaskStatusCollector statusCollector,
                            Consumer<TaskInfo> memoryReleaser) {
        this.vectorResultBuffer = vectorResultBuffer;
        this.rerankerExecutor = rerankerExecutor;
        this.statusCollector = statusCollector;
        this.memoryReleaser = memoryReleaser;
    }

    /** Returns the result buffer used for coordinator retrieval. */
    VectorSearchResultBuffer resultBuffer() {
        return vectorResultBuffer;
    }

    /**
     * Attempts to execute a vector search task via the Java-side ANN index path.
     * Returns true if the task was handled (caller should return), false if the
     * task should proceed through the standard native execution path.
     *
     * <p>Detection: checks for {@code pangrova.vector_search_operator} in session
     * properties. Execution: delegates to {@link VectorSearchTaskInterceptor}
     * which discovers {@link org.pangrova.spi.index.vector.VectorIndexLoader} via
     * ServiceLoader. If no loader is available, returns false (fall through).</p>
     */
    boolean tryExecuteVectorSearch(String taskId, TaskInfo info,
                                   TaskLifecycleManager.LifecycleCallback callback) {
        var request = info.request();
        if (request == null) {
            return false;
        }
        Map<String, String> sessionProps = request.sessionProperties();
        if (!VectorSearchTaskInterceptor.isVectorSearchTask(sessionProps)) {
            return false;
        }
        List<String> splitFilePaths = extractSplitFilePaths(request.splits());
        if (splitFilePaths.isEmpty()) {
            LOG.warn("event=vector_search_no_splits taskId={} queryId={}", taskId, info.queryId());
            return false;
        }
        var result = VectorSearchTaskInterceptor.tryExecute(sessionProps, splitFilePaths);
        if (result == null) {
            return false;
        }
        LOG.info("event=vector_search_completed taskId={} queryId={} neighbors={}",
                taskId, info.queryId(), result.neighbors().size());

        result = tryApplyReranking(result, sessionProps, taskId, info.queryId());

        // Serialize result to protobuf and buffer for coordinator retrieval
        byte[] serializedResult = serializeResult(result);
        vectorResultBuffer.put(taskId, serializedResult);

        // Report RUNNING status to coordinator (mirrors createAndStartExecutor flow)
        callback.transition(taskId, TaskManager.TaskState.RUNNING);
        if (statusCollector != null) {
            statusCollector.reportStatus(info, "RUNNING", null);
        }

        // Release memory budget before FINISHED so subsequent tasks are not starved
        memoryReleaser.accept(info);

        // Report FINISHED to coordinator so it can retrieve the buffered result
        if (statusCollector != null) {
            statusCollector.reportStatus(info, "FINISHED", null);
            statusCollector.logTaskCompleted(taskId);
        }
        callback.transition(taskId, TaskManager.TaskState.FINISHED);
        callback.notifyCompletion(info.queryId());
        return true;
    }

    /**
     * Serializes a VectorSearchResult into protobuf wire format
     * (VectorSearchTaskResult) for coordinator retrieval.
     * Applies DEFLATE compression for efficient transport (large K values).
     *
     * <p>Wire format: [4-byte magic 0x41455A43 "AEZC"] + [4-byte uncompressed length] + [deflated bytes]
     */
    static byte[] serializeResult(VectorSearchResult result) {
        org.pangrova.protocol.v1.VectorSearchTaskResult.Builder builder =
                org.pangrova.protocol.v1.VectorSearchTaskResult.newBuilder()
                        .setDataFilePath(result.dataFilePath());
        for (VectorSearchResult.ScoredRow row : result.neighbors()) {
            org.pangrova.protocol.v1.ScoredRowResult rowProto =
                    org.pangrova.protocol.v1.ScoredRowResult.newBuilder()
                            .setRowOffset(row.rowOffset())
                            .setDistance(row.distance())
                            .build();
            builder.addNeighbors(rowProto);
        }
        byte[] raw = builder.build().toByteArray();

        // Compress if payload is large enough to benefit (>256 bytes)
        if (raw.length <= 256) {
            return raw;
        }
        return compressPayload(raw);
    }

    /** DEFLATE compression with magic header for auto-detection on coordinator side. */
    private static byte[] compressPayload(byte[] raw) {
        java.util.zip.Deflater deflater = new java.util.zip.Deflater(
                java.util.zip.Deflater.BEST_SPEED);
        try {
            deflater.setInput(raw);
            deflater.finish();
            byte[] compressed = new byte[raw.length + 64]; // worst case
            int compressedLen = deflater.deflate(compressed);
            if (compressedLen >= raw.length) {
                // Compression didn't help — send raw
                return raw;
            }
            // Header: [magic:4B] [uncompressed_len:4B] [deflated_data]
            byte[] result = new byte[8 + compressedLen];
            writeIntBigEndian(result, 0, COMPRESSION_MAGIC);
            writeIntBigEndian(result, 4, raw.length);
            System.arraycopy(compressed, 0, result, 8, compressedLen);
            return result;
        } finally {
            deflater.end();
        }
    }

    /** Writes a 32-bit value as four big-endian bytes into {@code dest} at {@code offset}. */
    private static void writeIntBigEndian(byte[] dest, int offset, int value) {
        dest[offset] = (byte) (value >>> HIGH_BYTE_SHIFT);
        dest[offset + 1] = (byte) (value >>> 16);
        dest[offset + 2] = (byte) (value >>> 8);
        dest[offset + 3] = (byte) value;
    }

    private VectorSearchResult tryApplyReranking(VectorSearchResult result,
                                                 Map<String, String> sessionProps,
                                                 String taskId, String queryId) {
        String strategy = sessionProps.get("pangrova.reranker_strategy");
        if (strategy == null || strategy.isBlank()) {
            return result;
        }
        int topK = resolveTopK(sessionProps, result.neighbors().size());
        String strategyName = strategy.contains(":") ? strategy.substring(0, strategy.indexOf(':')) : strategy;
        Map<String, String> strategyProps = parseStrategyProperties(strategy);
        String queryText = sessionProps.getOrDefault("pangrova.reranker_query_text", "");

        List<ScoredCandidate> candidates = toCandidates(result);

        try {
            List<RankedResult> ranked =
                    rerankerExecutor.execute(strategyName, strategyProps, queryText, candidates, topK);
            List<VectorSearchResult.ScoredRow> rerankedRows = toScoredRows(ranked);

            LOG.info("event=reranker_applied taskId={} queryId={} strategy={} "
                            + "inputSize={} outputSize={}",
                    taskId, queryId, strategyName, candidates.size(), rerankedRows.size());
            return new VectorSearchResult(result.dataFilePath(), rerankedRows);
        } catch (Exception e) {
            LOG.warn("event=reranker_failed taskId={} queryId={} strategy={} error={}",
                    taskId, queryId, strategyName, e.getMessage());
            return result;
        }
    }

    /** Parses the reranker {@code top_k} session property, defaulting to the neighbor count. */
    private static int resolveTopK(Map<String, String> sessionProps, int neighborCount) {
        String topKStr = sessionProps.getOrDefault("pangrova.reranker_top_k", "0");
        int topK;
        try {
            topK = Integer.parseInt(topKStr);
        } catch (NumberFormatException e) {
            topK = neighborCount;
        }
        return topK > 0 ? topK : neighborCount;
    }

    private static List<ScoredCandidate> toCandidates(VectorSearchResult result) {
        List<ScoredCandidate> candidates = new ArrayList<>();
        for (var row : result.neighbors()) {
            candidates.add(new ScoredCandidate(row.rowOffset(), row.distance(), "vector"));
        }
        return candidates;
    }

    private static List<VectorSearchResult.ScoredRow> toScoredRows(List<RankedResult> ranked) {
        List<VectorSearchResult.ScoredRow> rerankedRows = new ArrayList<>(ranked.size());
        for (var r : ranked) {
            rerankedRows.add(new VectorSearchResult.ScoredRow(r.rowOffset(), r.score()));
        }
        return rerankedRows;
    }

    private static Map<String, String> parseStrategyProperties(String strategy) {
        Map<String, String> props = new HashMap<>();
        String[] parts = strategy.split(":");
        if (parts.length < 2) {
            return props;
        }
        switch (parts[0]) {
            case "rrf" -> props.put("k", parts[1]);
            case "weighted" -> putIfEnoughParts(props, parts, "weight1", "weight2");
            case "cross_encoder" -> putIfEnoughParts(props, parts, "modelName", "topN");
            case "cascade" -> props.put("stages", parts[1]);
            default -> {
                // Unknown strategy — no strategy-specific properties
            }
        }
        return props;
    }

    /** Copies {@code parts[1]} and {@code parts[2]} when at least three parts are present. */
    private static void putIfEnoughParts(Map<String, String> props, String[] parts,
                                         String firstKey, String secondKey) {
        if (parts.length >= 3) {
            props.put(firstKey, parts[1]);
            props.put(secondKey, parts[2]);
        }
    }

    private static List<String> extractSplitFilePaths(List<String> encodedSplits) {
        if (encodedSplits == null || encodedSplits.isEmpty()) {
            return List.of();
        }
        List<String> paths = new ArrayList<>(encodedSplits.size());
        for (String encoded : encodedSplits) {
            try {
                byte[] bytes = java.util.Base64.getDecoder().decode(encoded);
                org.pangrova.protocol.v1.SplitInfo split =
                        org.pangrova.protocol.v1.SplitInfo.parseFrom(bytes);
                if (!split.getFilePath().isEmpty()) {
                    paths.add(split.getFilePath());
                }
            } catch (Exception e) {
                LOG.debug("event=split_decode_failed error={}", e.getMessage());
            }
        }
        return paths;
    }
}
