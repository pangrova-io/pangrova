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
package org.pangrova.coordinator.query.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.pangrova.coordinator.ai.vector.GlobalTopKMerger;
import org.pangrova.coordinator.ai.vector.VectorSearchResultCollector;
import org.pangrova.coordinator.ai.vector.WorkerVectorResultFetcher;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.planner.vector.VectorSearchNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs global vector Top-K result collection after distributed dispatch.
 */
final class QueryVectorResultMerger {

    private static final Logger LOG = LoggerFactory.getLogger(
            QueryVectorResultMerger.class);
    private static final int COMPRESSION_MAGIC = 0x41455A43;

    private static final ExecutorService VECTOR_FETCH_EXECUTOR =
            Executors.newFixedThreadPool(8, r -> {
                Thread thread = new Thread(r, "vector-fetch-worker");
                thread.setDaemon(true);
                return thread;
            });

    private final WorkerManager workerManager;
    private final WorkerVectorResultFetcher vectorResultFetcher;

    QueryVectorResultMerger(WorkerManager workerManager) {
        this.workerManager = workerManager;
        this.vectorResultFetcher = new WorkerVectorResultFetcher();
    }

    DispatchResult finalizeDispatchResult(DispatchResult result,
                                          DispatchQueryContext ctx,
                                          String queryId) {
        if (ctx.vectorSearchNode().isEmpty()) {
            return result;
        }
        VectorSearchNode node = ctx.vectorSearchNode().get();
        DispatchResult vectorResult = result.withVectorSearchNode(node);
        LOG.info("event=global_vector_merge_point queryId={} table={} column={} "
                        + "topK={} metric={}",
                queryId, node.tableName(), node.vectorColumn(), node.topK(), node.metric());
        List<WorkerInfo> activeWorkers = workerManager.getActiveWorkers();
        if (activeWorkers.size() <= 1) {
            return vectorResult;
        }
        return mergeWorkerResults(vectorResult, node, activeWorkers, queryId);
    }

    private DispatchResult mergeWorkerResults(DispatchResult result,
                                              VectorSearchNode node,
                                              List<WorkerInfo> workers,
                                              String queryId) {
        List<List<GlobalTopKMerger.ScoredRow>> rows = fetchWorkerResults(
                workers, result.taskId(), queryId);
        if (rows.isEmpty()) {
            LOG.info("event=global_vector_merge_no_results queryId={} table={} topK={}",
                    queryId, node.tableName(), node.topK());
            return result;
        }
        var collector = new VectorSearchResultCollector(node);
        GlobalTopKMerger.MergeResult merged = collector.collectAndMerge(rows);
        LOG.info("event=global_vector_merge_complete queryId={} table={} topK={} "
                        + "resultSize={} workerCount={}",
                queryId, node.tableName(), node.topK(),
                merged.rows().size(), merged.workerCount());
        return result.withMergeResult(merged);
    }

    private List<List<GlobalTopKMerger.ScoredRow>> fetchWorkerResults(
            List<WorkerInfo> workers, String taskId, String queryId) {
        return workers.stream()
                .map(worker -> fetchWorkerResult(worker, taskId))
                .map(future -> awaitWorkerResult(future, queryId))
                .filter(rows -> !rows.isEmpty())
                .toList();
    }

    private CompletableFuture<List<GlobalTopKMerger.ScoredRow>> fetchWorkerResult(
            WorkerInfo worker, String taskId) {
        return CompletableFuture.supplyAsync(() -> {
            Optional<byte[]> bytes = vectorResultFetcher.fetch(
                    worker.host(), worker.httpPort(), taskId);
            return bytes.map(value -> parseWorkerResult(worker.workerId(), value))
                    .orElseGet(List::of);
        }, VECTOR_FETCH_EXECUTOR);
    }

    private List<GlobalTopKMerger.ScoredRow> awaitWorkerResult(
            CompletableFuture<List<GlobalTopKMerger.ScoredRow>> future,
            String queryId) {
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("event=vector_fetch_timeout queryId={}", queryId);
            return List.of();
        }
    }

    private static List<GlobalTopKMerger.ScoredRow> parseWorkerResult(
            String workerId, byte[] protoBytes) {
        try {
            org.pangrova.protocol.v1.VectorSearchTaskResult taskResult =
                    org.pangrova.protocol.v1.VectorSearchTaskResult.parseFrom(
                            maybeDecompress(protoBytes));
            return toScoredRows(workerId, taskResult);
        } catch (Exception e) {
            LOG.warn("event=vector_result_parse_failed workerId={} error={}",
                    workerId, e.getMessage());
            return List.of();
        }
    }

    private static List<GlobalTopKMerger.ScoredRow> toScoredRows(
            String workerId,
            org.pangrova.protocol.v1.VectorSearchTaskResult taskResult) {
        String filePath = taskResult.getDataFilePath();
        List<GlobalTopKMerger.ScoredRow> rows = new ArrayList<>();
        for (org.pangrova.protocol.v1.ScoredRowResult scored : taskResult.getNeighborsList()) {
            rows.add(new GlobalTopKMerger.ScoredRow(
                    workerId, scored.getRowOffset(), scored.getDistance(), filePath));
        }
        return rows;
    }

    private static byte[] maybeDecompress(byte[] data) {
        if (data.length < 8 || readInt(data, 0) != COMPRESSION_MAGIC) {
            return data;
        }
        int expectedLength = readInt(data, 4);
        byte[] output = new byte[expectedLength];
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(data, 8, data.length - 8);
            return inflatedPayload(inflater, output, expectedLength, data);
        } catch (DataFormatException e) {
            LOG.warn("event=vector_result_decompress_failed error={}", e.getMessage());
            return data;
        } finally {
            inflater.end();
        }
    }

    private static byte[] inflatedPayload(Inflater inflater, byte[] output,
                                          int expectedLength, byte[] original)
            throws DataFormatException {
        int inflated = inflater.inflate(output);
        if (inflated == expectedLength) {
            return output;
        }
        LOG.warn("event=vector_result_decompress_size_mismatch expected={} actual={}",
                expectedLength, inflated);
        return original;
    }

    private static int readInt(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24)
                | ((data[offset + 1] & 0xFF) << 16)
                | ((data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);
    }
}
