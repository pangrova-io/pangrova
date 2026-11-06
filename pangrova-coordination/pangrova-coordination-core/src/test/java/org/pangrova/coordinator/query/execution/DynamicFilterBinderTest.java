package org.pangrova.coordinator.query.execution;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.dispatch.adaptive.DynamicFilterBinder;
import org.pangrova.execution.core.binding.TaskRequestFactory;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.core.runtimefilter.RuntimeFilter;
import org.pangrova.planner.core.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.InjectRuntimeFilterRequest;
import org.pangrova.protocol.v2.ValueEncoding;
import org.pangrova.security.api.identity.UserIdentity;
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
        assertThat(payload.version()).isEqualTo(2);
        InjectRuntimeFilterRequest typed = parse(payload);
        assertThat(typed.getFiltersCount()).isEqualTo(1);
        assertThat(typed.getFilters(0).getColumnName()).isEqualTo("customer_id");
        assertThat(typed.getFilters(0).hasRange()).isTrue();
        assertThat(typed.getFilters(0).getRange().getLower().getInt64Value()).isEqualTo(10);
        assertThat(typed.getFilters(0).getRange().getUpper().getInt64Value()).isEqualTo(500);
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
        InjectRuntimeFilterRequest typed = parse(payload);
        assertThat(typed.getFilters(0).hasBloom()).isTrue();
        assertThat(typed.getFilters(0).getBloom().getValueEncoding())
                .isEqualTo(ValueEncoding.VALUE_ENCODING_UTF8);
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
        QuerySession session = createSession("q-single");
        byte[] planBytes = "plan".getBytes(StandardCharsets.UTF_8);
        StageResourceGrant limit = resourceGrant();

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                leafTask(session, planBytes, createStageNode(0), limit, filters));

        assertThat(request.taskRequest().getInitialRuntimeFiltersList()).hasSize(1);
        var bound = request.taskRequest().getInitialRuntimeFilters(0);
        assertThat(bound.getPlanNodeId()).isEqualTo("scan_id");
        assertThat(bound.getFilters(0).hasRange()).isTrue();
    }

    @Test
    void filtersAttachedToTaskRequest_multiStage() {
        RuntimeFilterRegistry registry = new RuntimeFilterRegistry();
        RuntimeFilter filter = RuntimeFilter.minMax("customer_id", 100, 999);
        registry.register("2", filter);

        var filters = DynamicFilterBinder.resolveFilters("2", "q-multi", registry);
        QuerySession session = createSession("q-multi");
        byte[] planBytes = "plan".getBytes(StandardCharsets.UTF_8);
        StageNode stage = createStageNode(2);
        StageResourceGrant limit = resourceGrant();

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                leafTask(session, planBytes, stage, limit, filters));

        assertThat(request.taskRequest().getInitialRuntimeFiltersList()).hasSize(1);
        assertThat(request.taskRequest().getInitialRuntimeFilters(0).getPlanNodeId())
                .isEqualTo("scan_customer_id");
        assertThat(request.taskRequest().getAttempt().getStageId()).isEqualTo(2);
    }

    @Test
    void noFilters_taskRequestHasEmptyFilterList() {
        QuerySession session = createSession("q-empty");
        byte[] planBytes = "plan".getBytes(StandardCharsets.UTF_8);
        StageResourceGrant limit = resourceGrant();

        SubmitTaskRequest request = TaskRequestFactory.buildLeafTaskRequest(
                leafTask(session, planBytes, createStageNode(0), limit, List.of()));

        assertThat(request.taskRequest().getInitialRuntimeFiltersList()).isEmpty();
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
        byte[] data = parse(payload).getFilters(0)
                .getBloom().getSerializedFilter().toByteArray();
        SplitBlockBloomFilter restored = SplitBlockBloomFilter.readFrom(data);
        assertThat(restored.mightContain("alpha")).isTrue();
        assertThat(restored.mightContain("beta")).isTrue();
        assertThat(restored.mightContain("gamma")).isTrue();
    }

    private static InjectRuntimeFilterRequest parse(DynamicFilterPayload payload) {
        try {
            return InjectRuntimeFilterRequest.parseFrom(payload.filterData());
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new AssertionError(failure);
        }
    }

    private static QuerySession createSession(String queryId) {
        UserIdentity identity = new UserIdentity(
                "testUser", Set.of(), Map.of(), java.time.Instant.now());
        return new QuerySession(queryId, Map.of(), identity, "default-tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    private static StageResourceGrant resourceGrant() {
        return StageResourceGrant.newBuilder()
                .setGrantId("unbound-test-grant")
                .setNativeMemoryBytes(1024L)
                .setCpuSlots(2)
                .build();
    }

    private static TaskRequestFactory.LeafTask leafTask(
            QuerySession session,
            byte[] planBytes,
            StageNode stage,
            StageResourceGrant resourceGrant,
            List<DynamicFilterPayload> filters) {
        return new TaskRequestFactory.LeafTask(
                new org.pangrova.execution.core.authority.StageExecutionContext.Live(session),
                planBytes, new org.pangrova.execution.api.stage.TaskStage(stage.stageId(),
                        org.pangrova.execution.api.stage.TaskPurpose.valueOf(stage.taskType())), 0, resourceGrant,
                List.of(), filters, null, List.of(),
                org.pangrova.execution.api.deadline.StageTaskDeadline.after(
                        java.time.Duration.ofMinutes(5), java.time.Instant.now()),
                org.pangrova.protocol.rest.task.TaskAttemptDescriptor.of(1L, 0),
                List.of(), null, List.of());
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
        return new StageNode(stageId, new org.pangrova.planner.core.analysis.PlanFragment(dummyRel, -1L, -1L), 1, "COMPUTE", List.of());
    }
}
