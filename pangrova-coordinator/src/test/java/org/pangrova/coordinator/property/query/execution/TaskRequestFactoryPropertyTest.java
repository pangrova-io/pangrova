package org.pangrova.coordinator.property.query.execution;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.TaskRequestFactory;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.spi.security.UserIdentity;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for TaskRequestFactory taskId generation.
 *
 * <p><b>Validates: Requirements 1.1, 1.2, 2.1, 2.2</b></p>
 */
class TaskRequestFactoryPropertyTest {

    /**
     * Property 1: Every SubmitTaskRequest produced by buildLeafTaskRequest
     * has a non-null, non-blank taskId that is a valid UUID string.
     *
     * <p><b>Validates: Requirements 1.2</b></p>
     */
    @Property(tries = 10)
    void leafTaskRequestAlwaysHasValidTaskId(
            @ForAll("queryIds") String queryId,
            @ForAll("planBytes") byte[] planBytes) {
        CoordinatorSessionContext session = createSession(queryId);
        StageResourceGrant limit = resourceGrant();

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                leafTask(session, planBytes, createStageNode(0), 0, limit));

        String taskId = request.taskRequest().getAttempt().getTaskId();
        assertThat(taskId).isNotBlank();
        assertThat(java.util.UUID.fromString(taskId)).isNotNull();
        assertThat(request.taskRequest().getSplitsList()).isEmpty();
    }

    /**
     * Property 1 (continued): Every SubmitTaskRequest produced by buildLeafTaskRequest
     * has a non-null, non-blank taskId that is a valid UUID string.
     *
     * <p><b>Validates: Requirements 1.2</b></p>
     */
    @Property(tries = 10)
    void stageTaskRequestAlwaysHasValidTaskId(
            @ForAll("queryIds") String queryId,
            @ForAll("planBytes") byte[] planBytes,
            @ForAll @IntRange(min = 0, max = 100) int stageId,
            @ForAll @IntRange(min = 0, max = 50) int partition) {
        CoordinatorSessionContext session = createSession(queryId);
        StageResourceGrant limit = resourceGrant();

        org.pangrova.coordinator.query.stage.StageNode stage = createStageNode(stageId);

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                leafTask(session, planBytes, stage, partition, limit));

        String taskId = request.taskRequest().getAttempt().getTaskId();
        assertThat(taskId).isNotBlank();
        assertThat(java.util.UUID.fromString(taskId)).isNotNull();
        assertThat(request.taskRequest().getSplitsList()).isEmpty();
    }

    /**
     * Property 1 (uniqueness): Multiple calls to buildLeafTaskRequest
     * with the same inputs produce distinct taskIds.
     *
     * <p><b>Validates: Requirements 2.1</b></p>
     */
    @Property(tries = 10)
    void leafTaskRequestTaskIdsAreUnique(
            @ForAll("queryIds") String queryId,
            @ForAll @IntRange(min = 2, max = 50) int count) {
        CoordinatorSessionContext session = createSession(queryId);
        StageResourceGrant limit = resourceGrant();
        byte[] planBytes = "plan".getBytes(StandardCharsets.UTF_8);

        Set<String> taskIds = new HashSet<>();
        for (int i = 0; i < count; i++) {
            SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                    leafTask(session, planBytes, createStageNode(0), 0, limit));
            taskIds.add(request.taskRequest().getAttempt().getTaskId());
        }

        assertThat(taskIds).hasSize(count);
    }

    @Provide
    Arbitrary<String> queryIds() {
        return Arbitraries.strings()
                .alpha()
                .ofMinLength(1)
                .ofMaxLength(20)
                .map(s -> "q-" + s);
    }

    @Provide
    Arbitrary<byte[]> planBytes() {
        return Arbitraries.bytes()
                .array(byte[].class)
                .ofMinSize(1)
                .ofMaxSize(64);
    }

    private static TaskRequestFactory.LeafTask leafTask(
            CoordinatorSessionContext session,
            byte[] planBytes,
            org.pangrova.coordinator.query.stage.StageNode stage,
            int partition,
            StageResourceGrant resourceGrant) {
        return new TaskRequestFactory.LeafTask(
                new org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext.Live(session),
                planBytes, stage, partition, resourceGrant,
                List.of(), List.of(), null, List.of(),
                org.pangrova.coordinator.query.execution.dispatch.dag.control.StageTaskDeadline.after(
                        java.time.Duration.ofMinutes(5), java.time.Instant.now()),
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1L, 0),
                List.of(), null, List.of());
    }

    private static CoordinatorSessionContext createSession(String queryId) {
        UserIdentity identity = new UserIdentity("testUser", Set.of(), Map.of(), java.time.Instant.now());
        return new CoordinatorSessionContext(queryId, Map.of(), identity, "default-tenant");
    }

    private static StageResourceGrant resourceGrant() {
        return StageResourceGrant.newBuilder()
                .setGrantId("unbound-property-grant")
                .setNativeMemoryBytes(1024L)
                .setCpuSlots(2)
                .build();
    }

    private static org.pangrova.coordinator.query.stage.StageNode createStageNode(int stageId) {
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
                new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);
        org.apache.calcite.plan.RelOptCluster cluster =
                org.apache.calcite.plan.RelOptCluster.create(
                        new org.apache.calcite.plan.volcano.VolcanoPlanner(), rexBuilder);
        org.apache.calcite.rel.RelNode dummyRel =
                org.apache.calcite.rel.logical.LogicalValues.createEmpty(cluster,
                        typeFactory.builder().build());
        return new org.pangrova.coordinator.query.stage.StageNode(
                stageId, new org.pangrova.planner.PlanFragment(dummyRel, -1L, -1L), 1, "COMPUTE", List.of());
    }
}
