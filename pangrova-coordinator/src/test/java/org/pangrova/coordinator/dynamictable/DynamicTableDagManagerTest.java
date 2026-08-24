package org.pangrova.coordinator.dynamictable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link DynamicTableDagManager}.
 *
 * <p><b>Validates: Requirements 6</b></p>
 */
class DynamicTableDagManagerTest {

    private DynamicTableDagManager dag;

    @BeforeEach
    void setUp() {
        dag = new DynamicTableDagManager();
    }

    // ── Register dependencies ───────────────────────────────────────

    @Test
    void registerDependencies_createsEdges() {
        dag.registerDependencies("dt_b", Set.of("source_a"));
        assertThat(dag.getDirectDownstreams("source_a")).containsExactly("dt_b");
    }

    @Test
    void registerDependencies_multipleUpstreams() {
        dag.registerDependencies("dt_c", Set.of("source_a", "source_b"));
        assertThat(dag.getDirectDownstreams("source_a")).contains("dt_c");
        assertThat(dag.getDirectDownstreams("source_b")).contains("dt_c");
    }

    // ── Cycle detection ─────────────────────────────────────────────

    @Test
    void cycleDetection_selfReference() {
        assertThatThrownBy(() -> dag.registerDependencies("dt_a", Set.of("dt_a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cycle");
    }

    @Test
    void cycleDetection_directCycle() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        assertThatThrownBy(() -> dag.registerDependencies("dt_a", Set.of("dt_b")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cycle");
    }

    @Test
    void cycleDetection_transitiveCycle() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        dag.registerDependencies("dt_c", Set.of("dt_b"));
        assertThatThrownBy(() -> dag.registerDependencies("dt_a", Set.of("dt_c")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cycle");
    }

    @Test
    void noCycle_validChain() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        dag.registerDependencies("dt_c", Set.of("dt_b"));
        assertThat(dag.getDirectDownstreams("dt_a")).contains("dt_b");
        assertThat(dag.getDirectDownstreams("dt_b")).contains("dt_c");
    }

    // ── Topological sort ────────────────────────────────────────────

    @Test
    void topologicalOrder_linearChain() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        dag.registerDependencies("dt_c", Set.of("dt_b"));

        List<String> order = dag.topologicalOrder();

        assertThat(order.indexOf("dt_a")).isLessThan(order.indexOf("dt_b"));
        assertThat(order.indexOf("dt_b")).isLessThan(order.indexOf("dt_c"));
    }

    @Test
    void topologicalOrder_diamond() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        dag.registerDependencies("dt_c", Set.of("dt_a"));
        dag.registerDependencies("dt_d", Set.of("dt_b", "dt_c"));

        List<String> order = dag.topologicalOrder();

        assertThat(order.indexOf("dt_a")).isLessThan(order.indexOf("dt_b"));
        assertThat(order.indexOf("dt_a")).isLessThan(order.indexOf("dt_c"));
        assertThat(order.indexOf("dt_b")).isLessThan(order.indexOf("dt_d"));
        assertThat(order.indexOf("dt_c")).isLessThan(order.indexOf("dt_d"));
    }

    @Test
    void topologicalOrder_emptyGraph() {
        assertThat(dag.topologicalOrder()).isEmpty();
    }

    // ── getDependents (transitive) ──────────────────────────────────

    @Test
    void getDependents_transitiveDownstreams() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        dag.registerDependencies("dt_c", Set.of("dt_b"));
        dag.registerDependencies("dt_d", Set.of("dt_c"));

        Set<String> dependents = dag.getDependents("dt_a");

        assertThat(dependents).containsExactlyInAnyOrder("dt_b", "dt_c", "dt_d");
    }

    @Test
    void getDependents_noDownstreams() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        assertThat(dag.getDependents("dt_b")).isEmpty();
    }

    // ── Cascade removal ─────────────────────────────────────────────

    @Test
    void removeWithCascade_removesNodeAndAllDownstreams() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        dag.registerDependencies("dt_c", Set.of("dt_b"));

        Set<String> removed = dag.removeWithCascade("dt_b");

        assertThat(removed).containsExactlyInAnyOrder("dt_b", "dt_c");
        assertThat(dag.getDirectDownstreams("dt_a")).isEmpty();
        assertThat(dag.getDirectDownstreams("dt_b")).isEmpty();
    }

    @Test
    void removeWithCascade_leafNode() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));

        Set<String> removed = dag.removeWithCascade("dt_b");

        assertThat(removed).containsExactly("dt_b");
        assertThat(dag.getDirectDownstreams("dt_a")).isEmpty();
    }

    // ── Recover ─────────────────────────────────────────────────────

    @Test
    void recover_rebuildsFromMap() {
        Map<String, Set<String>> deps = new LinkedHashMap<>();
        deps.put("dt_b", Set.of("dt_a"));
        deps.put("dt_c", Set.of("dt_b"));

        dag.recover(deps);

        assertThat(dag.getDirectDownstreams("dt_a")).contains("dt_b");
        assertThat(dag.getDirectDownstreams("dt_b")).contains("dt_c");
    }

    @Test
    void recover_clearsExistingState() {
        dag.registerDependencies("old_dt", Set.of("old_source"));
        dag.recover(Map.of("new_dt", Set.of("new_source")));

        assertThat(dag.getDirectDownstreams("old_source")).isEmpty();
        assertThat(dag.getDirectDownstreams("new_source")).contains("new_dt");
    }

    // ── getDepth ────────────────────────────────────────────────────

    @Test
    void getDepth_rootNode() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        assertThat(dag.getDepth("dt_a")).isZero();
    }

    @Test
    void getDepth_linearChain() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        dag.registerDependencies("dt_c", Set.of("dt_b"));

        assertThat(dag.getDepth("dt_a")).isZero();
        assertThat(dag.getDepth("dt_b")).isEqualTo(1);
        assertThat(dag.getDepth("dt_c")).isEqualTo(2);
    }

    @Test
    void getDepth_diamond() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        dag.registerDependencies("dt_c", Set.of("dt_a"));
        dag.registerDependencies("dt_d", Set.of("dt_b", "dt_c"));

        assertThat(dag.getDepth("dt_d")).isEqualTo(2);
    }

    @Test
    void getDepth_unknownNode() {
        assertThat(dag.getDepth("nonexistent")).isZero();
    }

    // ── DagProvider contract ────────────────────────────────────────

    @Test
    void implementsDagProvider() {
        dag.registerDependencies("dt_b", Set.of("dt_a"));
        org.pangrova.coordinator.dynamictable.refresh.DagProvider provider = dag;
        assertThat(provider.getDirectDownstreams("dt_a")).contains("dt_b");
    }
}
