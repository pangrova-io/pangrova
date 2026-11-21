package org.pangrova.coordinator.property.query.execution;

import java.util.stream.IntStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.execution.task.DispatchResult;
import org.pangrova.execution.task.StageDispatchRecord;
import org.pangrova.coordinator.query.execution.dispatch.dag.ExchangeInputTestAccess;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.protocol.rest.task.exchange.ShuffleEndpointDescriptor;
import org.pangrova.protocol.rest.task.exchange.ShufflePartitionTicketDescriptor;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.ExchangePartitionSource;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for non-leaf stage dispatch parallelism (P0-FPV-9).
 * Verifies hash-exchange plans with downstream parallelism N create
 * N consumer tasks pulling partitions 0..N-1.
 *
 * <p><b>Validates: Requirements A3.1</b></p>
 */
class DispatchParallelismPropertyTest {

    /**
     * Exchange sources carry the requested partitionId.
     * <p><b>Validates: Requirements A3.1</b></p>
     */
    @Property(tries = 10)
    void exchangeSourcesCarryRequestedPartitionId(
            @ForAll @IntRange(min = 1, max = 32) int par,
            @ForAll @IntRange(min = 0, max = 31) int raw) {
        int pid = raw % par;
        StageDAG dag = twoStageDAG(1, par);
        List<StageDispatchRecord> d = List.of(rec(0, 0, "t0", "h1"));
        List<ExchangePartitionSource> r = call(
                node(1, par, "OUTPUT"), dag, d, pid);
        assertThat(r).isNotEmpty();
        r.forEach(s -> assertThat(s.getOutputPartitionId()).isEqualTo(pid));
    }

    /**
     * All partitions 0..N-1 are covered across consumer tasks.
     * <p><b>Validates: Requirements A3.1</b></p>
     */
    @Property(tries = 10)
    void allPartitionsCovered(
            @ForAll @IntRange(min = 2, max = 16) int par) {
        StageDAG dag = twoStageDAG(1, par);
        List<StageDispatchRecord> d = List.of(rec(0, 0, "t0", "h1"));
        Set<Integer> seen = new HashSet<>();
        for (int p = 0; p < par; p++) {
            List<ExchangePartitionSource> r = call(
                    node(1, par, "OUTPUT"), dag, d, p);
            assertThat(r).hasSize(1);
            seen.add(r.get(0).getOutputPartitionId());
        }
        Set<Integer> exp = new HashSet<>();
        for (int i = 0; i < par; i++) exp.add(i);
        assertThat(seen).isEqualTo(exp);
    }

    /**
     * Leaf stages return empty exchange sources.
     * <p><b>Validates: Requirements A3.1</b></p>
     */
    @Property(tries = 10)
    void leafStagesEmpty(
            @ForAll @IntRange(min = 1, max = 16) int par,
            @ForAll @IntRange(min = 0, max = 15) int pid) {
        StageNode leaf = node(0, par, "COMPUTE");
        StageDAG dag = StageDAG.fromNodes(
                List.of(leaf), Map.of(), List.of());
        assertThat(call(leaf, dag, List.of(), pid)).isEmpty();
    }

    /**
     * Multiple upstream tasks all carry the same partitionId.
     * <p><b>Validates: Requirements A3.1</b></p>
     */
    @Property(tries = 10)
    void multiUpstreamSamePartition(
            @ForAll @IntRange(min = 2, max = 8) int upCnt,
            @ForAll @IntRange(min = 1, max = 8) int par) {
        StageDAG dag = twoStageDAG(upCnt, par);
        List<StageDispatchRecord> d = new ArrayList<>();
        for (int i = 0; i < upCnt; i++) {
            d.add(rec(0, i, "t" + i, "h" + i));
        }
        int tp = par / 2;
        List<ExchangePartitionSource> r = call(
                node(1, par, "OUTPUT"), dag, d, tp);
        assertThat(r).hasSize(upCnt);
        r.forEach(s -> assertThat(s.getOutputPartitionId()).isEqualTo(tp));
    }

    private static List<ExchangePartitionSource> call(
            StageNode stg, StageDAG dag,
            List<StageDispatchRecord> disp, int pid) {
        return ExchangeInputTestAccess.build(stg, dag, disp, pid).stream()
                .flatMap(input -> input.getDirect().getSourcesList().stream())
                .toList();
    }

    private static StageDispatchRecord rec(int sid, int sourcePartition, String tid, String h) {
        long expiresAt = System.currentTimeMillis() + 600_000L;
        var tickets = IntStream.range(0, 32)
                .mapToObj(partition -> new ShufflePartitionTicketDescriptor(
                        partition, java.util.Base64.getEncoder().encodeToString(
                                ("ticket-" + tid + "-" + partition).getBytes(
                                        java.nio.charset.StandardCharsets.UTF_8)), expiresAt))
                .toList();
        var ep = new ResultEndpoint(h, 8080, "tk", "http");
        var dr = new DispatchResult(h, tid, ep).withShuffleEndpoint(
                new ShuffleEndpointDescriptor(h, 9090, "", tickets));
        SubmitTaskRequest request =
                org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                        tid, "query", sid, sourcePartition,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return new StageDispatchRecord(sid, dr, 8080, 9090, request);
    }

    private static StageDAG twoStageDAG(int upP, int dnP) {
        StageNode up = node(0, upP, "COMPUTE");
        StageNode dn = node(1, dnP, "OUTPUT");
        var edge = new StageEdge(0, 1,
                org.pangrova.planner.enums.ShuffleType.HASH,
                List.of(0));
        return StageDAG.fromNodes(List.of(up, dn),
                Map.of(1, List.of(0)), List.of(edge));
    }

    private static StageNode node(int id, int p, String t) {
        var tf = new SqlTypeFactoryImpl(
                RelDataTypeSystem.DEFAULT);
        var cl = RelOptCluster.create(
                new VolcanoPlanner(), new RexBuilder(tf));
        RelNode rel = LogicalValues.createEmpty(
                cl, tf.builder().build());
        return new StageNode(id, new org.pangrova.planner.analysis.PlanFragment(rel, -1L, -1L), p, t, List.of());
    }
}
