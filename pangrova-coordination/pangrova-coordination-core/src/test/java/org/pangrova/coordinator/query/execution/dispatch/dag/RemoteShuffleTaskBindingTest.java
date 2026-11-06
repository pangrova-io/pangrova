package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.execution.core.binding.TaskRequestFactory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.logical.LogicalValues;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.planner.core.analysis.PlanFragment;
import org.pangrova.execution.core.authority.StageExecutionContext;
import org.pangrova.protocol.rest.config.ShuffleConfig;
import org.pangrova.protocol.rest.task.ExecutionOwnerIdentity;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.shuffle.api.ShuffleAccess;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.Test;

class RemoteShuffleTaskBindingTest {

    @Test
    void coordinatorAllocationIsCarriedAndStampedWithTaskOwner() {
        var request = TaskRequestFactory.buildLeafTaskRequest(
                new TaskRequestFactory.LeafTask(
                        new StageExecutionContext.Live(session()), new byte[] {1}, new org.pangrova.execution.api.stage.TaskStage(stage().stageId(),
                        org.pangrova.execution.api.stage.TaskPurpose.valueOf(stage().taskType())), 0, resourceGrant(),
                        List.of(), List.of(),
                        new ShuffleConfig(
                                "ARROW_BATCH", "zstd", 1024,
                                "MEMORY_AND_REMOTE", 8, List.of(0)),
                        List.of(), org.pangrova.execution.api.deadline.StageTaskDeadline.after(
                                java.time.Duration.ofSeconds(30), java.time.Instant.now()),
                        TaskAttemptDescriptor.of(7L, 1), List.of(), shuffleHandle(), List.of()));

        var allocation = request.taskRequest().getExecutionOptions()
                .getShuffle().getExecutionBinding();
        var wire = request.taskRequest().getExecutionOptions().getShuffle();
        assertThat(wire.getSerde()).isEqualTo(org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH);
        assertThat(wire.getCompressionCodec()).isEqualTo("zstd");
        assertThat(wire.getBatchSize()).isEqualTo(1024);
        assertThat(allocation.getShuffleId()).isEqualTo("shuffle-1");
        assertThat(allocation.getPlanEpoch()).isEqualTo(7L);
        assertThat(allocation.getWriteCapability().getExpiresAtEpochMs())
                .isLessThan(System.currentTimeMillis());
        assertThat(allocation.getEndpoints(0).getHost()).isEqualTo("shuffle-1");
        assertThat(allocation.getEndpoints(0).getTlsIdentity()).isEqualTo("shuffle-1");
        assertThat(allocation.hasExecutionOwner()).isFalse();

        var dispatcher = new RemoteTaskDispatcher(
                CircuitBreakerPolicy.noOp(), new NoOpTracingProvider());
        dispatcher.setControlEpochSupplier(() -> 41L);
        dispatcher.setExecutionOwnerResolver(ignored ->
                new ExecutionOwnerIdentity("coordinator-1", "incarnation-1", 23L));
        var bound = dispatcher.bindControlIdentity(request).taskRequest();

        assertThat(bound.getAttempt().getControlEpoch()).isEqualTo(41L);
        assertThat(bound.getExecutionOptions().getShuffle().getExecutionBinding()
                .getExecutionOwner()).isEqualTo(
                        bound.getAttempt().getExecutionOwner());
    }

    private static QuerySession session() {
        return new QuerySession(
                "query-1", Map.of(),
                new UserIdentity("user-1", Set.of(), Map.of(), java.time.Instant.EPOCH),
                "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    private static StageNode stage() {
        var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var cluster = RelOptCluster.create(
                new VolcanoPlanner(), new RexBuilder(typeFactory));
        var relation = LogicalValues.createEmpty(
                cluster, typeFactory.builder().build());
        return new StageNode(
                3, new PlanFragment(relation, -1L, -1L),
                1, "COMPUTE", List.of());
    }

    private static StageResourceGrant resourceGrant() {
        return StageResourceGrant.newBuilder()
                .setGrantId("grant-1")
                .setNativeMemoryBytes(1024L)
                .setCpuSlots(1)
                .build();
    }

    private static ShuffleHandle shuffleHandle() {
        // The stage ticket is a Coordinator-only placeholder until task grant.
        long expiry = System.currentTimeMillis() - 1L;
        return new ShuffleHandle(
                "shuffle-1",
                List.of(new ShuffleServerEndpoint(
                        "shuffle-1", 9400, 0, 8, "shuffle-1")),
                new ShuffleServiceAddress("shuffle-1", 9400, "shuffle-1"),
                new ShuffleAccess(new byte[] {1}, expiry),
                new ShuffleAccess(new byte[] {2}, expiry),
                new ShuffleAccess(new byte[] {3}, expiry),
                new ShuffleAccess(new byte[] {4}, expiry),
                1, new byte[32]);
    }
}
