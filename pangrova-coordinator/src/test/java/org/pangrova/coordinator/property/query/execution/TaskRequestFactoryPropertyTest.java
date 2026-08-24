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
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
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
        ResourceLimitPayload limit = new ResourceLimitPayload(1024L, 2, 0L, "/tmp");

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                session, planBytes, createStageNode(0), 0, limit, List.of(), List.of());

        assertThat(request.taskId()).isNotNull();
        assertThat(request.taskId()).isNotBlank();
        assertThat(java.util.UUID.fromString(request.taskId())).isNotNull();
        assertThat(request.splitMode()).isEqualTo("PULL");
        assertThat(request.splits()).isEmpty();
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
        ResourceLimitPayload limit = new ResourceLimitPayload(1024L, 2, 0L, "/tmp");

        org.pangrova.coordinator.query.stage.StageNode stage = createStageNode(stageId);

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                session, planBytes, stage, partition, limit, List.of(), List.of());

        assertThat(request.taskId()).isNotNull();
        assertThat(request.taskId()).isNotBlank();
        assertThat(java.util.UUID.fromString(request.taskId())).isNotNull();
        assertThat(request.splitMode()).isEqualTo("PULL");
        assertThat(request.splits()).isEmpty();
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
        ResourceLimitPayload limit = new ResourceLimitPayload(1024L, 2, 0L, "/tmp");
        byte[] planBytes = "plan".getBytes(StandardCharsets.UTF_8);

        Set<String> taskIds = new HashSet<>();
        for (int i = 0; i < count; i++) {
            SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                    session, planBytes, createStageNode(0), 0, limit, List.of(), List.of());
            taskIds.add(request.taskId());
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

    private static CoordinatorSessionContext createSession(String queryId) {
        UserIdentity identity = new UserIdentity("testUser", Set.of(), Map.of(), java.time.Instant.now());
        return new CoordinatorSessionContext(queryId, Map.of(), identity, "default-tenant");
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
