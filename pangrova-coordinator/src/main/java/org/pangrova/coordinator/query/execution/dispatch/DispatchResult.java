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
package org.pangrova.coordinator.query.execution.dispatch;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.Optional;

import org.pangrova.coordinator.ai.vector.GlobalTopKMerger;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.planner.vector.VectorSearchNode;
import org.pangrova.protocol.rest.query.ResultEndpoint;

/**
 * Holds the outcome of dispatching a task to a worker.
 * Cross-package contract: produced by scheduler, consumed by query execution and result service.
 *
 * @param workerHost       hostname of the worker (for backward compatibility)
 * @param taskId           dispatched task identifier
 * @param resultEndpoint   endpoint for result retrieval
 * @param workerId         unique worker identifier for precise routing (nullable for backward compat)
 * @param vectorSearchNode vector search plan metadata, present when the dispatched query
 *                         contains a vector search operation (empty otherwise)
 * @param mergeResult      global Top-K merge result, present after coordinator-side merge
 *                         of per-worker vector search results (empty otherwise)
 */
public record DispatchResult(String workerHost, String taskId, ResultEndpoint resultEndpoint,
                              String workerId, Optional<VectorSearchNode> vectorSearchNode,
                              Optional<GlobalTopKMerger.MergeResult> mergeResult) {

    /** Backward-compatible constructor without workerId or vectorSearchNode. */
    public DispatchResult(String workerHost, String taskId, ResultEndpoint resultEndpoint) {
        this(workerHost, taskId, resultEndpoint, null, Optional.empty(), Optional.empty());
    }

    /** Backward-compatible constructor without vectorSearchNode. */
    public DispatchResult(String workerHost, String taskId, ResultEndpoint resultEndpoint, String workerId) {
        this(workerHost, taskId, resultEndpoint, workerId, Optional.empty(), Optional.empty());
    }

    /** Backward-compatible constructor without mergeResult. */
    public DispatchResult(String workerHost, String taskId, ResultEndpoint resultEndpoint,
                          String workerId, Optional<VectorSearchNode> vectorSearchNode) {
        this(workerHost, taskId, resultEndpoint, workerId, vectorSearchNode, Optional.empty());
    }

    public DispatchResult {
        checkNotNull(workerHost, "workerHost must not be null");
        checkNotNull(taskId, "taskId must not be null");
        checkNotNull(resultEndpoint, "resultEndpoint must not be null");
        vectorSearchNode = vectorSearchNode != null ? vectorSearchNode : Optional.empty();
        mergeResult = mergeResult != null ? mergeResult : Optional.empty();
    }

    /** Returns a copy with the vector search node attached. */
    public DispatchResult withVectorSearchNode(VectorSearchNode node) {
        return new DispatchResult(workerHost, taskId, resultEndpoint, workerId,
                Optional.ofNullable(node), mergeResult);
    }

    /** Returns a copy with the global merge result attached. */
    public DispatchResult withMergeResult(GlobalTopKMerger.MergeResult result) {
        return new DispatchResult(workerHost, taskId, resultEndpoint, workerId,
                vectorSearchNode, Optional.ofNullable(result));
    }

    /** Converts a coordinator-local dispatch result to a standard DispatchResult. */
    public static DispatchResult fromLocal(CoordinatorResultService.LocalDispatchResult local) {
        return new DispatchResult(local.workerHost(), local.taskId(), local.resultEndpoint());
    }
}
