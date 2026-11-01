package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.DAGStagePlanBinder;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.PlanFragment;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.spi.security.UserIdentity;

import static org.assertj.core.api.Assertions.assertThat;

class TaskRequestResultColumnTypeTest {

    @Test
    void bindsSeparateModelDimensionsWithoutClassifyingOrdinaryArrays() {
        var compiler = new SqlCompiler();
        var relation = compiler.compileToRelNode(
                "SELECT AI_EMBED('catalog.first', 'a') AS first, "
                        + "ARRAY[1.0, 2.0] AS ordinary, "
                        + "AI_EMBED('catalog.second', 'b') AS second");

        var columns = DAGStagePlanBinder.bindResultColumnTypes(
                new PlanFragment(relation, -1, -1),
                Map.of("catalog.first", VectorLogicalType.create(3),
                        "catalog.second", VectorLogicalType.create(5)));

        assertThat(columns).extracting(ResultColumnLogicalType::getColumnIndex)
                .containsExactly(0, 2);
        assertThat(columns).extracting(column ->
                VectorLogicalType.requireDimension(column.getLogicalType()))
                .containsExactly(3, 5);
    }

    @Test
    void carriesIndexedLogicalResultTypeInCanonicalRequest() {
        var compiler = new SqlCompiler();
        var relation = compiler.compileToRelNode("SELECT 1");
        var stage = new StageNode(1, new PlanFragment(relation, -1, -1),
                1, "OUTPUT", List.of());
        var type = ResultColumnLogicalType.newBuilder()
                .setColumnIndex(0)
                .setLogicalType(VectorLogicalType.create(3))
                .build();
        var session = new CoordinatorSessionContext(
                "query-result-type", Map.of(),
                new UserIdentity("user", Set.of(), Map.of(), Instant.EPOCH),
                "tenant");
        var request = TaskRequestFactory.buildLeafTaskRequest(
                new TaskRequestFactory.LeafTask(
                        new StageExecutionContext.Live(session), new byte[] {1}, stage, 0,
                        StageResourceGrant.newBuilder()
                                .setGrantId("grant")
                                .setNativeMemoryBytes(1024)
                                .setCpuSlots(1)
                                .build(),
                        List.of(), List.of(), null, List.of(),
                        org.pangrova.coordinator.query.execution.dispatch.dag.control.StageTaskDeadline.after(
                                java.time.Duration.ofSeconds(30), java.time.Instant.now()),
                        TaskAttemptDescriptor.of(1L, 0), List.of(), null,
                        List.of(type)));

        assertThat(request.taskRequest().getResultColumnTypesList())
                .containsExactly(type);
    }
}
