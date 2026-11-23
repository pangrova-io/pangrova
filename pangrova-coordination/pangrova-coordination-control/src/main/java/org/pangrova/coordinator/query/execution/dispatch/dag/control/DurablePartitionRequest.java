package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.shuffle.api.coordinator.ExternalShuffleDescriptorEncoder;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.execution.api.stage.TaskStage;
import org.pangrova.execution.api.enums.TaskPurpose;
import org.pangrova.execution.deadline.TaskLeaseDeadlines;

import org.pangrova.generation.execution.binding.GenerationPartitionBindings;
import org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan;
import org.pangrova.execution.binding.TaskRequestFactory;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PreparedPartition;
import org.pangrova.protocol.rest.config.ShuffleConfig;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.v2.ExchangeInputBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ModelSnapshot;
import org.pangrova.common.api.annotation.Internal;

import java.time.Instant;
import java.util.List;

/** Pure immutable preparation for an actual leased partition through the ordinary leaf request factory. */
@Internal
public final class DurablePartitionRequest {
    private DurablePartitionRequest() {}

    /** Existing external allocation identity and short-lived access; never stored inside the immutable graph. */
    public record ExchangeAccess(long planEpoch, List<ExchangeInputBinding> inputs,
            ShuffleConfig output, ShuffleHandle allocation, ExternalShuffleDescriptorEncoder descriptorEncoder) {
        public ExchangeAccess {
            if (planEpoch <= 0) throw new IllegalArgumentException("External allocation plan epoch must be positive");
            inputs = List.copyOf(inputs);
        }
    }

    /** Shares the validated template, plan bytes and checksum for one durable stage. */
    public static final class StageRequestPlan {
        private final GenerationPartitionBindings.StageBindings bindings;
        private final TaskRequestFactory.FrozenStagePlan plan;

        /** Freezes the canonical request plan before taking any partition claim. */
        public StageRequestPlan(StagePlan stage, List<ModelSnapshot> models) {
            bindings = new GenerationPartitionBindings.StageBindings(stage, models);
            plan = new TaskRequestFactory.FrozenStagePlan(bindings.planBytes());
        }
    }

    /** Builds no alternate protocol: all task security, budgets, options and attempt fields use the common factory. */
    public static SubmitTaskRequest prepare(PreparedPartition partition, ExchangeAccess exchange, Instant now,
            org.pangrova.observability.api.tracing.ExecutionTraceContext traceContext) {
        var graph = partition.authority().dispatchSpec().stageGraph();
        var stage = graph.requireStage(partition.lease().stageId());
        return prepare(partition, new StageRequestPlan(stage, graph.modelSnapshots()), exchange, now, traceContext);
    }

    /** Binds current claim evidence to the already validated plan for this stage. */
    public static SubmitTaskRequest prepare(PreparedPartition partition, StageRequestPlan requestPlan,
            ExchangeAccess exchange, Instant now,
            org.pangrova.observability.api.tracing.ExecutionTraceContext traceContext) {
        StageExchangeAccessValidator.validate(partition, exchange);
        var lease = partition.lease();
        var spec = partition.authority().dispatchSpec();
        var stage = spec.stageGraph().requireStage(lease.stageId());
        var prepared = GenerationPartitionBindings.bind(partition, requestPlan.bindings, now);
        TaskPurpose taskType = switch (stage.pageKind()) {
            case RELATIONAL_COMPUTE -> TaskPurpose.COMPUTE;
            case RELATIONAL_PARTITION -> TaskPurpose.OUTPUT;
            case SOURCE_MAP, DEPENDENCY_REDUCE, GENERATION_FINALIZE -> TaskPurpose.BACKGROUND_BUILD;
        };
        var plan = new TaskStage(stage.stageId(), taskType);
        return TaskRequestFactory.buildLeafTaskRequest(new TaskRequestFactory.LeafTask(
                new StageExecutionContext.Deferred(partition.authority(), lease, traceContext),
                requestPlan.plan, plan, lease.partitionId(),
                resourceGrant(partition, prepared.operatorBindings()), exchange.inputs(),
                exchange.output(), prepared.splits(),
                TaskLeaseDeadlines.forPartition(lease, spec, now),
                new TaskAttemptDescriptor(exchange.planEpoch(), lease.attemptId(), lease.fencingToken()),
                prepared.operatorBindings(), exchange.allocation(), requestPlan.bindings.resultColumnTypes()));
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
