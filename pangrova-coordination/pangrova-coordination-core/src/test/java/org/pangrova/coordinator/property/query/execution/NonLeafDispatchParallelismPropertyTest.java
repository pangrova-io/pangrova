package org.pangrova.coordinator.property.query.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.query.core.task.DispatchResult;
import org.pangrova.query.core.task.StageDispatchRecord;
import org.pangrova.coordinator.query.execution.dispatch.dag.ExchangeInputTestAccess;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.transport.api.result.ResultEndpoint;
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
 * Property-based tests for non-leaf stage dispatch parallelism (P0-FPV-9).
 * <p><b>Validates: Requirements A3.1</b></p>
 */
class NonLeafDispatchParallelismPropertyTest {

    /** Exchange sources carry the requested partitionId. */
    @Property(tries = 10)
    void exchangeSourcesCarryRequestedPartitionId(
            @ForAll @IntRange(min = 1, max = 32) int par,
            @ForAll @IntRange(min = 0, max = 31) int raw) {
        int pid = raw % par;
        StageDAG dag = twoStageDAG(1, par);
        List<StageDispatchRecord> disp = List.of(rec(0, 0, "t0", "h1"));
        List<ExchangePartitionSource> res = call(node(1, par, "OUTPUT"), dag, disp, pid);
        assertThat(res).isNotEmpty();
        res.forEach(s -> assertThat(s.getOutputPartitionId()).isEqualTo(pid));
    }


    /** All partitions 0..N-1 are covered across consumer tasks. */
    @Property(tries = 10)
    void allPartitionsCovered(@ForAll @IntRange(min = 2, max = 16) int par) {
        StageDAG dag = twoStageDAG(1, par);
        List<StageDispatchRecord> disp = List.of(rec(0, 0, "t0", "h1"));
        Set<Integer> seen = new HashSet<>();
        for (int p = 0; p < par; p++) {
            List<ExchangePartitionSource> r = call(node(1, par, "OUTPUT"), dag, disp, p);
            assertThat(r).hasSize(1);
            seen.add(r.get(0).getOutputPartitionId());
        }
        Set<Integer> exp = new HashSet<>();
        for (int i = 0; i < par; i++) exp.add(i);
        assertThat(seen).isEqualTo(exp);
    }

    /** Leaf stages return empty exchange sources. */
    @Property(tries = 10)
    void leafStagesEmpty(
            @ForAll @IntRange(min = 1, max = 16) int par,
            @ForAll @IntRange(min = 0, max = 15) int pid) {
        StageNode leaf = node(0, par, "COMPUTE");
        StageDAG dag = StageDAG.fromNodes(List.of(leaf), Map.of(), List.of());
        assertThat(call(leaf, dag, List.of(), pid)).isEmpty();
    }

    /** Multiple upstream tasks all carry the same partitionId. */
    @Property(tries = 10)
    void multiUpstreamSamePartition(
            @ForAll @IntRange(min = 2, max = 8) int upCnt,
            @ForAll @IntRange(min = 1, max = 8) int par) {
        StageDAG dag = twoStageDAG(upCnt, par);
        List<StageDispatchRecord> disp = new ArrayList<>();
        for (int i = 0; i < upCnt; i++) {
            disp.add(rec(0, i, "t" + i, "h" + i));
        }
        int tp = par / 2;
        List<ExchangePartitionSource> r = call(node(1, par, "OUTPUT"), dag, disp, tp);
        assertThat(r).hasSize(upCnt);
        r.forEach(s -> assertThat(s.getOutputPartitionId()).isEqualTo(tp));
    }

    // --- helpers ---

    private static List<ExchangePartitionSource> call(
            StageNode stage, StageDAG dag, List<StageDispatchRecord> disp, int pid) {
        return ExchangeInputTestAccess.build(stage, dag, disp, pid).stream()
                .flatMap(input -> input.getDirect().getSourcesList().stream())
                .toList();
    }

    private static StageDispatchRecord rec(int sid, int sourcePartition, String tid, String host) {
        long expiresAt = System.currentTimeMillis() + 600_000L;
        var tickets = IntStream.range(0, 32)
                .mapToObj(partition -> new ShufflePartitionTicketDescriptor(
                        partition, java.util.Base64.getEncoder().encodeToString(
                                ("ticket-" + tid + "-" + partition).getBytes(
                                        java.nio.charset.StandardCharsets.UTF_8)), expiresAt))
                .toList();
        ResultEndpoint ep = new ResultEndpoint(host, 8080, "tk", "http");
        DispatchResult dr = new DispatchResult(host, tid, ep).withShuffleEndpoint(
                new ShuffleEndpointDescriptor(host, 9090, "", tickets));
        SubmitTaskRequest request =
                org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                        tid, "query", sid, sourcePartition,
                        org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                        "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return new StageDispatchRecord(sid, dr, 8080, 9090, request);
    }

    private static StageDAG twoStageDAG(int upPar, int downPar) {
        StageNode up = node(0, upPar, "COMPUTE");
        StageNode dn = node(1, downPar, "OUTPUT");
        StageEdge edge = new StageEdge(0, 1,
                org.pangrova.planner.core.analysis.ShuffleType.HASH, List.of(0));
        return StageDAG.fromNodes(List.of(up, dn),
                Map.of(1, List.of(0)), List.of(edge));
    }

    private static StageNode node(int id, int par, String type) {
        SqlTypeFactoryImpl tf = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cl = RelOptCluster.create(
                new VolcanoPlanner(), new RexBuilder(tf));
        RelNode rel = LogicalValues.createEmpty(cl, tf.builder().build());
        return new StageNode(id, new org.pangrova.planner.core.analysis.PlanFragment(rel, -1L, -1L), par, type, List.of());
    }
}
