package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.coordinator.artifact.template.ArtifactPartitionBindings;
import org.pangrova.coordinator.artifact.template.PersistedStagePlan;
import org.pangrova.coordinator.query.execution.dispatch.dag.TaskRequestFactory;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.indexing.store.jdbc.JdbcArtifactStageExecutionStore.PreparedPartition;
import org.pangrova.protocol.rest.config.ShuffleConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.shuffle.api.registration.ShuffleHandle;
import org.pangrova.spi.annotation.Internal;

import java.time.Instant;
import java.util.List;

/** Pure immutable preparation for an actual leased partition through the ordinary leaf request factory. */
@Internal
public final class DurablePartitionRequest {
    private DurablePartitionRequest() {}

    /** Existing external allocation identity and short-lived access; never stored inside the immutable graph. */
    public record ExchangeAccess(long planEpoch, List<ExchangeInputBinding> inputs,
            ShuffleConfig output, ShuffleHandle allocation) {
        public ExchangeAccess {
            if (planEpoch <= 0) throw new IllegalArgumentException("External allocation plan epoch must be positive");
            inputs = List.copyOf(inputs);
        }
    }

    /** Builds no alternate protocol: all task security, budgets, options and attempt fields use the common factory. */
    public static SubmitTaskRequest prepare(PreparedPartition partition, ExchangeAccess exchange, Instant now) {
        return prepare(partition, exchange, now,
                org.pangrova.observability.tracing.ExecutionTraceContext.capture());
    }

    /** Retains the same correlation snapshot across all partitions of one admitted replay. */
    public static SubmitTaskRequest prepare(PreparedPartition partition, ExchangeAccess exchange, Instant now,
            org.pangrova.observability.tracing.ExecutionTraceContext traceContext) {
        StageExchangeAccessValidator.validate(partition, exchange);
        var lease = partition.lease();
        var spec = partition.authority().dispatchSpec();
        var stage = spec.stageGraph().requireStage(lease.stageId());
        var bound = ArtifactPartitionBindings.bind(partition, now);
        String taskType = switch (stage.pageKind()) {
            case RELATIONAL_COMPUTE -> "COMPUTE";
            case RELATIONAL_PARTITION -> "OUTPUT";
            case SOURCE_MAP, DEPENDENCY_REDUCE, GENERATION_FINALIZE -> "BACKGROUND_BUILD";
        };
        var plan = new StageNode(stage.stageId(), new PersistedStagePlan(stage),
                stage.parallelism(), taskType, List.of());
        return TaskRequestFactory.buildLeafTaskRequest(new TaskRequestFactory.LeafTask(
                new StageExecutionContext.Deferred(partition.authority(), lease, traceContext), bound.planBytes(), plan,
                lease.partitionId(), resourceGrant(partition, bound.operatorBindings()), exchange.inputs(), List.of(),
                exchange.output(), lease.splits().stream().map(split -> split.canonicalSplitBase64()).toList(),
                StageTaskDeadline.forPartition(lease, spec, now),
                new TaskAttemptDescriptor(exchange.planEpoch(), lease.attemptId(), lease.fencingToken()),
                bound.operatorBindings(), exchange.allocation(), bound.resultColumnTypes()));
    }

    private static StageResourceGrant resourceGrant(PreparedPartition partition, List<OperatorBinding> bindings) {
        var spec = partition.authority().dispatchSpec();
        long reads = 0;
        long writes = 0;
        for (var binding : bindings) {
            reads = Math.addExact(reads, binding.getResources().getObjectStoreReadBytes());
            writes = Math.addExact(writes, binding.getResources().getObjectStoreWriteBytes());
        }
        return StageResourceGrant.newBuilder().setCpuSlots(spec.maxThreads())
                .setNativeMemoryBytes(spec.maxMemoryBytes()).setSpillBytes(spec.maxSpillBytes())
                .setObjectStoreReadBytes(reads).setObjectStoreWriteBytes(writes).build();
    }
}
