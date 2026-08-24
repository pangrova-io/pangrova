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
package org.pangrova.coordinator.ai.vector;

import java.util.List;
import java.util.Objects;

import org.pangrova.planner.vector.VectorSearchNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates collection of per-worker vector search results and delegates
 * to {@link GlobalTopKMerger} for global Top-K computation.
 *
 * <p>In a distributed vector search, each worker executes a local ANN scan
 * and returns its local Top-K results. This collector gathers those partial
 * results and merges them into the global Top-K.</p>
 *
 * <p>Current status: the merge logic is fully implemented, but actual result
 * collection from workers requires a worker-side HTTP endpoint that exposes
 * the {@code VectorSearchResultBuffer}. Until that endpoint exists, the
 * collector can only be used with results that have already been fetched
 * by the caller.</p>
 *
 * <p>Thread-safe: all state is confined to method scope.</p>
 */
public final class VectorSearchResultCollector {

    private static final Logger LOG = LoggerFactory.getLogger(VectorSearchResultCollector.class);

    private final VectorSearchNode searchNode;

    public VectorSearchResultCollector(VectorSearchNode searchNode) {
        this.searchNode = Objects.requireNonNull(searchNode, "searchNode required");
    }

    /**
     * Merges pre-fetched per-worker vector search results into a global Top-K.
     *
     * <p>Each inner list represents one worker's local Top-K results. The merge
     * uses a max-heap to efficiently select the global Top-K across all workers,
     * with deduplication by file path and row ID.</p>
     *
     * @param perWorkerResults list of per-worker scored rows, one list per worker
     * @return the merged global Top-K result
     */
    public GlobalTopKMerger.MergeResult collectAndMerge(
            List<List<GlobalTopKMerger.ScoredRow>> perWorkerResults) {
        Objects.requireNonNull(perWorkerResults, "perWorkerResults required");
        if (perWorkerResults.isEmpty()) {
            LOG.debug("event=vector_collect_empty table={} topK={}",
                    searchNode.tableName(), searchNode.topK());
            return new GlobalTopKMerger.MergeResult(List.of(), 0, 0, 0);
        }

        LOG.info("event=vector_collect_merge_start table={} column={} topK={} "
                        + "metric={} workerCount={}",
                searchNode.tableName(), searchNode.vectorColumn(),
                searchNode.topK(), searchNode.metric(),
                perWorkerResults.size());

        GlobalTopKMerger.MergeResult merged = GlobalTopKMerger.merge(
                perWorkerResults, searchNode.topK());

        LOG.info("event=vector_collect_merge_complete table={} topK={} "
                        + "resultSize={} totalScanned={} workerCount={}",
                searchNode.tableName(), searchNode.topK(),
                merged.rows().size(), merged.totalScanned(),
                merged.workerCount());

        return merged;
    }

    /**
     * Returns the vector search plan node associated with this collector.
     */
    public VectorSearchNode searchNode() {
        return searchNode;
    }

    /**
     * Creates a collector from a dispatch result's vector search metadata,
     * returning empty if the dispatch was not a vector search query.
     *
     * @param vectorSearchNode the vector search node from dispatch context
     * @return a collector, or empty if no vector search metadata is present
     */
    public static java.util.Optional<VectorSearchResultCollector> fromSearchNode(
            java.util.Optional<VectorSearchNode> vectorSearchNode) {
        return vectorSearchNode.map(VectorSearchResultCollector::new);
    }
}
