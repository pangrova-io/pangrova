package org.pangrova.coordinator.query.execution;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.DynamicFilterBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.TaskRequestFactory;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.runtimefilter.RuntimeFilter;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that DPP runtime filters from {@link RuntimeFilterRegistry} are correctly
 * bound to target stages and serialized as {@link DynamicFilterPayload} in dispatch output.
 *
 * <p><b>Validates: Requirements 1.41, 2.41</b></p>
 */
class DynamicFilterBinderTest {

    @Test
    void resolveFilters_returnsEmptyWhenRegistryIsNull() {
        List<DynamicFilterPayload> filters = DynamicFilterBinder.resolveFilters(
                "1", "q-1", null);
        assertThat(filters).isEmpty();
    }

    @Test
    void resolveFilters_returnsEmptyWhenNoFilterRegistered() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        List<DynamicFilterPayload> filters = DynamicFilterBinder.resolveFilters(
                "1", "q-1", registry);
        assertThat(filters).isEmpty();
    }

    @Test
    void resolveFilters_bindsMinMaxFilterToCorrectStage() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilter minMax = RuntimeFilter.minMax("customer_id", 10, 500);
        registry.register("2", minMax);

        List<DynamicFilterPayload> filters = DynamicFilterBinder.resolveFilters(
                "2", "q-test", registry);

        assertThat(filters).hasSize(1);
        DynamicFilterPayload payload = filters.get(0);
        assertThat(payload.filterId()).isEqualTo("dpp-2");
        assertThat(payload.filterType()).isEqualTo("MIN_MAX");
        assertThat(payload.targetStageId()).isEqualTo("2");
        assertThat(payload.version()).isEqualTo(1);
        String data = new String(payload.filterData(), StandardCharsets.UTF_8);
        assertThat(data).isEqualTo("customer_id|10|500");
    }

    @Test
    void resolveFilters_bindsBloomFilterToCorrectStage() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(100, 0.03)
                .put("val1")
                .put("val2")
                .build();
        RuntimeFilter bloom = RuntimeFilter.bloom("order_id", sbbf);
        registry.register("3", bloom);

        List<DynamicFilterPayload> filters = DynamicFilterBinder.resolveFilters(
                "3", "q-bloom", registry);

        assertThat(filters).hasSize(1);
        DynamicFilterPayload payload = filters.get(0);
        assertThat(payload.filterId()).isEqualTo("dpp-3");
        assertThat(payload.filterType()).isEqualTo("BLOOM");
        assertThat(payload.targetStageId()).isEqualTo("3");
        assertThat(payload.filterData()).isNotEmpty();
    }

    @Test
    void resolveFilters_doesNotMatchWrongStage() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilter filter = RuntimeFilter.minMax("col", 1, 100);
        registry.register("5", filter);

        List<DynamicFilterPayload> filters = DynamicFilterBinder.resolveFilters(
                "3", "q-miss", registry);
        assertThat(filters).isEmpty();
    }

    @Test
    void filtersAttachedToTaskRequest_leafStage() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilter filter = RuntimeFilter.minMax("id", 1, 1000);
        registry.register("0", filter);

        var filters = DynamicFilterBinder.resolveFilters("0", "q-single", registry);
        CoordinatorSessionContext session = createSession("q-single");
        byte[] planBytes = "plan".getBytes(StandardCharsets.UTF_8);
        ResourceLimitPayload limit = new ResourceLimitPayload(1024L, 2, 0L, "");

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                session, planBytes, createStageNode(0), 0, limit, List.of(), filters);

        assertThat(request.dynamicFilters()).hasSize(1);
        assertThat(request.dynamicFilters().get(0).targetStageId()).isEqualTo("0");
        assertThat(request.dynamicFilters().get(0).filterType()).isEqualTo("MIN_MAX");
    }

    @Test
    void filtersAttachedToTaskRequest_multiStage() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilter filter = RuntimeFilter.minMax("customer_id", 100, 999);
        registry.register("2", filter);

        var filters = DynamicFilterBinder.resolveFilters("2", "q-multi", registry);
        CoordinatorSessionContext session = createSession("q-multi");
        byte[] planBytes = "plan".getBytes(StandardCharsets.UTF_8);
        StageNode stage = createStageNode(2);
        ResourceLimitPayload limit = new ResourceLimitPayload(1024L, 2, 0L, "");

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                session, planBytes, stage, 0, limit, List.of(), filters);

        assertThat(request.dynamicFilters()).hasSize(1);
        assertThat(request.dynamicFilters().get(0).filterId()).isEqualTo("dpp-2");
        assertThat(request.dynamicFilters().get(0).targetStageId()).isEqualTo("2");
        assertThat(request.stageId()).isEqualTo(2);
    }

    @Test
    void noFilters_taskRequestHasEmptyFilterList() {
        CoordinatorSessionContext session = createSession("q-empty");
        byte[] planBytes = "plan".getBytes(StandardCharsets.UTF_8);
        ResourceLimitPayload limit = new ResourceLimitPayload(1024L, 2, 0L, "");

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                session, planBytes, createStageNode(0), 0, limit, List.of(), List.of());

        assertThat(request.dynamicFilters()).isEmpty();
    }

    @Test
    void bloomFilter_roundTrip_sbbfSerializationFormat() {
        SplitBlockBloomFilter sbbf = SplitBlockBloomFilter.builder(50, 0.01)
                .put("alpha")
                .put("beta")
                .put("gamma")
                .build();
        RuntimeFilter bloom = RuntimeFilter.bloom("col", sbbf);

        DynamicFilterPayload payload = DynamicFilterBinder.toPayload("7", bloom);

        // Verify SBBF binary can be deserialized back
        byte[] data = payload.filterData();
        SplitBlockBloomFilter restored = SplitBlockBloomFilter.readFrom(data);
        assertThat(restored.mightContain("alpha")).isTrue();
        assertThat(restored.mightContain("beta")).isTrue();
        assertThat(restored.mightContain("gamma")).isTrue();
    }

    private static CoordinatorSessionContext createSession(String queryId) {
        UserIdentity identity = new UserIdentity(
                "testUser", Set.of(), Map.of(), java.time.Instant.now());
        return new CoordinatorSessionContext(queryId, Map.of(), identity, "default-tenant");
    }

    private static StageNode createStageNode(int stageId) {
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
                new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
                        org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        org.apache.calcite.rex.RexBuilder rexBuilder =
                new org.apache.calcite.rex.RexBuilder(typeFactory);
        org.apache.calcite.plan.RelOptCluster cluster =
                org.apache.calcite.plan.RelOptCluster.create(
                        new org.apache.calcite.plan.volcano.VolcanoPlanner(), rexBuilder);
        org.apache.calcite.rel.RelNode dummyRel =
                org.apache.calcite.rel.logical.LogicalValues.createEmpty(
                        cluster, typeFactory.builder().build());
        return new StageNode(stageId, new org.pangrova.planner.PlanFragment(dummyRel, -1L, -1L), 1, "COMPUTE", List.of());
    }
}
