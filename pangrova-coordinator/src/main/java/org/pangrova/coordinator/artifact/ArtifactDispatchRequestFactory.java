package org.pangrova.coordinator.artifact;

import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurablePartitionRequest;
import org.pangrova.indexing.control.ArtifactBuildLease;
import org.pangrova.indexing.store.jdbc.JdbcArtifactStageExecutionStore;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.OperatorBinding;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Binds an issued page through the same current-PG preparation and ordinary leaf request path. */
final class ArtifactDispatchRequestFactory {
    private final JdbcArtifactStageExecutionStore stages;

    ArtifactDispatchRequestFactory(JdbcArtifactStageExecutionStore stages) {
        this.stages = Objects.requireNonNull(stages, "stages");
    }

    PreparedRequest prepare(ArtifactBuildLease lease, Instant now) {
        return stages.prepareLeasedPartition(lease, now, evidence -> {
            var graph = evidence.authority().dispatchSpec().stageGraph();
            if (!graph.exchanges().isEmpty()) {
                throw new IllegalStateException(
                        "Allocated durable exchange work must use its owning ordinary stage DAG");
            }
            var request = DurablePartitionRequest.prepare(evidence,
                    new DurablePartitionRequest.ExchangeAccess(lease.executionAnchor().fencingToken(),
                            List.of(), null, null), now);
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
