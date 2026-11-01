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

import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.client.protocol.ResultEndpoint;
import org.pangrova.protocol.rest.task.exchange.ShuffleEndpointDescriptor;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Control-plane outcome of dispatching one task.
 *
 * <p>It intentionally contains endpoints and identities only. Candidate rows,
 * scores, fusion state and Top-K results belong to the Native data plane.</p>
 */
public record DispatchResult(
        String workerHost,
        String taskId,
        ResultEndpoint resultEndpoint,
        String workerId,
        ShuffleEndpointDescriptor shuffleEndpoint) {

    public DispatchResult(String workerHost, String taskId, ResultEndpoint resultEndpoint) {
        this(workerHost, taskId, resultEndpoint, null, null);
    }

    public DispatchResult(
            String workerHost, String taskId, ResultEndpoint resultEndpoint,
            String workerId) {
        this(workerHost, taskId, resultEndpoint, workerId, null);
    }

    public DispatchResult {
        checkNotNull(workerHost, "workerHost must not be null");
        checkNotNull(taskId, "taskId must not be null");
        // Intermediate and write tasks have no client-readable result stream.
    }

    /** Returns a copy carrying the producer-issued shuffle endpoint. */
    public DispatchResult withShuffleEndpoint(ShuffleEndpointDescriptor endpoint) {
        return new DispatchResult(
                workerHost, taskId, resultEndpoint, workerId, endpoint);
    }

    /** Converts a bounded control-plane statement result to a dispatch descriptor. */
    public static DispatchResult fromControlPlane(
            CoordinatorResultService.ControlPlaneDispatchResult controlPlane) {
        return new DispatchResult(
                controlPlane.workerHost(), controlPlane.taskId(), controlPlane.resultEndpoint());
    }
}
