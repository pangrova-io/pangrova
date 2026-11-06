package org.pangrova.coordinator.generation;

import org.pangrova.observability.spi.tracing.ExecutionTraceContextFactory;

import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurablePartitionRequest;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.OperatorBinding;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Binds an issued page through current-authority preparation and the ordinary leaf request path. */
final class GenerationDispatchRequestFactory {
    private final ExecutionTraceContextFactory traceContexts;
    private final GenerationStageExecutionStore stages;

    GenerationDispatchRequestFactory(GenerationStageExecutionStore stages, ExecutionTraceContextFactory traceContexts) {
        this.traceContexts = java.util.Objects.requireNonNull(traceContexts, "traceContexts");
        this.stages = Objects.requireNonNull(stages, "stages");
    }

    PreparedRequest prepare(GenerationLease lease, Instant now) {
        return stages.prepareLeasedPartition(lease, now, evidence -> {
            var graph = evidence.authority().dispatchSpec().stageGraph();
            if (!graph.exchanges().isEmpty()) {
                throw new IllegalStateException(
                        "Allocated durable exchange work must use its owning ordinary stage DAG");
            }
            var request = DurablePartitionRequest.prepare(evidence,
                    new DurablePartitionRequest.ExchangeAccess(lease.executionAnchor().fencingToken(),
                            List.of(), null, null), now, traceContexts.capture());
            return new PreparedRequest(request.taskRequest().getAttempt().getTaskId(),
                    evidence.authority().dispatchSpec().maxMemoryBytes(),
                    request.taskRequest().getOperatorBindingsList(), request);
        });
    }

    record PreparedRequest(String taskId, long maxMemoryBytes, List<OperatorBinding> bindings,
            SubmitTaskRequest request) {
        PreparedRequest {
            bindings = List.copyOf(bindings);
        }
    }
}
