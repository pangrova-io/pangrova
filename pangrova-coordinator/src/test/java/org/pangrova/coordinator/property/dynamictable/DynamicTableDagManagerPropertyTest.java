package org.pangrova.coordinator.property.dynamictable;

import net.jqwik.api.*;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;
import org.pangrova.coordinator.dynamictable.DynamicTableDagManager;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property-based tests for {@link DynamicTableDagManager}.
 *
 * <p><b>Validates: Requirements 6</b></p>
 */
class DynamicTableDagManagerPropertyTest {

    /** Property 1: Topological sort satisfies all edge constraints. */
    @Property(tries = 10)
    void topologicalSort_satisfiesAllEdgeConstraints(
            @ForAll("acyclicGraphs") AcyclicGraph graph) {
        DynamicTableDagManager dag = new DynamicTableDagManager();
        for (var entry : graph.edges().entrySet()) {
            dag.registerDependencies(entry.getKey(), entry.getValue());
        }

        List<String> order = dag.topologicalOrder();
        Map<String, Integer> position = new HashMap<>();
        for (int i = 0; i < order.size(); i++) {
            position.put(order.get(i), i);
        }

        for (var entry : graph.edges().entrySet()) {
            String downstream = entry.getKey();
            for (String upstream : entry.getValue()) {
                if (position.containsKey(upstream) && position.containsKey(downstream)) {
                    assertThat(position.get(upstream))
                            .as("upstream %s before downstream %s", upstream, downstream)
                            .isLessThan(position.get(downstream));
                }
            }
        }
    }

    /** Property 2: Cycle detection always rejects cycles. */
    @Property(tries = 10)
    void cycleDetection_alwaysRejectsCycles(
            @ForAll @AlphaChars @StringLength(min = 1, max = 5) String a,
            @ForAll @AlphaChars @StringLength(min = 1, max = 5) String b) {
        String nodeA = "A_" + a;
        String nodeB = "B_" + b;
        DynamicTableDagManager dag = new DynamicTableDagManager();
        dag.registerDependencies(nodeB, Set.of(nodeA));

        assertThatThrownBy(() -> dag.registerDependencies(nodeA, Set.of(nodeB)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cycle");
    }

    /** Property 3: removeWithCascade leaves no residual edges. */
    @Property(tries = 10)
    void removeWithCascade_noResidualEdges(
            @ForAll("acyclicGraphs") AcyclicGraph graph) {
        DynamicTableDagManager dag = new DynamicTableDagManager();
        for (var entry : graph.edges().entrySet()) {
            dag.registerDependencies(entry.getKey(), entry.getValue());
        }

        if (graph.edges().isEmpty()) {
            return;
        }

        String toRemove = graph.edges().keySet().iterator().next();
        Set<String> removed = dag.removeWithCascade(toRemove);

        for (String node : removed) {
            assertThat(dag.getDirectDownstreams(node)).isEmpty();
        }
    }

    // ── Arbitrary providers ─────────────────────────────────────────

    @Provide
    Arbitrary<AcyclicGraph> acyclicGraphs() {
        return Arbitraries.integers().between(1, 6).flatMap(nodeCount -> {
            List<String> nodes = new ArrayList<>();
            for (int i = 0; i < nodeCount; i++) {
                nodes.add("node_" + i);
            }
            return Arbitraries.just(nodes).map(n -> {
                Map<String, Set<String>> edgeMap = new LinkedHashMap<>();
                Random rng = new Random();
                for (int i = 1; i < n.size(); i++) {
                    Set<String> deps = new HashSet<>();
                    for (int j = 0; j < i; j++) {
                        if (rng.nextBoolean()) {
                            deps.add(n.get(j));
                        }
                    }
                    if (!deps.isEmpty()) {
                        edgeMap.put(n.get(i), deps);
                    }
                }
                return new AcyclicGraph(edgeMap);
            });
        });
    }

    record AcyclicGraph(Map<String, Set<String>> edges) {}
}
