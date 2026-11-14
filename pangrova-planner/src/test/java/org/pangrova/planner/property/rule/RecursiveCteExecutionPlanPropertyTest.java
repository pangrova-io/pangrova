package org.pangrova.planner.property.rule;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.parser.api.ast.CteQuery.CycleClause;
import org.pangrova.parser.api.ast.CteQuery.SearchClause;
import org.pangrova.parser.api.enums.CteSearchOrder;
import org.pangrova.parser.api.ast.LiteralValue;
import org.pangrova.planner.rule.RecursiveCteConfig;
import org.pangrova.planner.rule.RecursiveCteExecutionPlan;
import org.pangrova.planner.exception.RecursiveCteIterationLimitException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests for recursive CTE execution plan.
 *
 * <p><b>Validates: Requirements C.1</b></p>
 */
class RecursiveCteExecutionPlanPropertyTest {

    @Property
    void seedRowsAlwaysAppearInResult(@ForAll @IntRange(min = 1, max = 20) int seedSize) {
        List<Integer> seeds = new ArrayList<>();
        for (int i = 0; i < seedSize; i++) {
            seeds.add(i);
        }

        var plan = new RecursiveCteExecutionPlan(
                "test_cte", RecursiveCteConfig.defaults(),
                Optional.empty(), Optional.empty());

        List<Integer> result = plan.execute(seeds, batch -> List.of());
        assertThat(result).containsAll(seeds);
    }

    @Property
    void emptyRecursiveStepProducesOnlySeedRows(@ForAll @IntRange(min = 0, max = 10) int seedSize) {
        List<Integer> seeds = new ArrayList<>();
        for (int i = 0; i < seedSize; i++) {
            seeds.add(i);
        }

        var plan = new RecursiveCteExecutionPlan(
                "test_cte", RecursiveCteConfig.defaults(),
                Optional.empty(), Optional.empty());

        List<Integer> result = plan.execute(seeds, batch -> List.of());
        assertThat(result).isEqualTo(seeds);
    }

    @Property
    void iterationLimitEnforcedForInfiniteRecursion(
            @ForAll @IntRange(min = 1, max = 50) int limit) {
        var config = new RecursiveCteConfig(limit);
        var plan = new RecursiveCteExecutionPlan(
                "infinite_cte", config, Optional.empty(), Optional.empty());

        assertThatThrownBy(() -> plan.execute(List.of(1), batch -> batch))
                .isInstanceOf(RecursiveCteIterationLimitException.class)
                .satisfies(ex -> {
                    var cteEx = (RecursiveCteIterationLimitException) ex;
                    assertThat(cteEx.cteName()).isEqualTo("infinite_cte");
                    assertThat(cteEx.iterationLimit()).isEqualTo(limit);
                });
    }

    @Property
    void cycleDetectionPreventsInfiniteLoop(@ForAll @IntRange(min = 1, max = 10) int graphSize) {
        // Build a cycle: 0 → 1 → 2 → ... → graphSize-1 → 0
        var cycleClause = new CycleClause(
                List.of("id"), "is_cycle",
                new LiteralValue.StringValue("1"), new LiteralValue.StringValue("0"), "path");

        var plan = new RecursiveCteExecutionPlan(
                "cycle_cte", RecursiveCteConfig.defaults(),
                Optional.empty(), Optional.of(cycleClause));

        List<Integer> result = plan.execute(
                List.of(0),
                batch -> batch.stream()
                        .map(n -> (n + 1) % graphSize)
                        .toList());

        // With cycle detection, each node visited at most once
        Set<Integer> unique = new HashSet<>(result);
        assertThat(unique).hasSize(result.size());
        assertThat(result.size()).isLessThanOrEqualTo(graphSize);
    }

    @Property
    void depthFirstProducesDepthOrderedTraversal(
            @ForAll @IntRange(min = 2, max = 5) int depth) {
        // Binary tree: each node n has children 2n+1, 2n+2
        var searchClause = new SearchClause(
                CteSearchOrder.DEPTH_FIRST, List.of(), "seq");

        var plan = new RecursiveCteExecutionPlan(
                "dfs_cte", new RecursiveCteConfig(1000),
                Optional.of(searchClause), Optional.empty());

        List<Integer> result = plan.execute(
                List.of(0),
                batch -> {
                    List<Integer> children = new ArrayList<>();
                    for (int n : batch) {
                        int left = 2 * n + 1;
                        int right = 2 * n + 2;
                        if (left < (1 << depth) - 1) {
                            children.add(left);
                        }
                        if (right < (1 << depth) - 1) {
                            children.add(right);
                        }
                    }
                    return children;
                });

        // DFS: left child appears before right sibling's subtree
        assertThat(result).isNotEmpty();
        assertThat(result.get(0)).isEqualTo(0);
        if (result.size() > 2) {
            // Node 1 (left child of 0) should appear before node 2 (right child of 0)
            assertThat(result.indexOf(1)).isLessThan(result.indexOf(2));
        }
    }

    @Property
    void breadthFirstProducesLevelOrderedTraversal(
            @ForAll @IntRange(min = 3, max = 5) int depth) {
        // Binary tree: each node n has children 2n+1, 2n+2
        var plan = new RecursiveCteExecutionPlan(
                "bfs_cte", new RecursiveCteConfig(1000),
                Optional.empty(), Optional.empty());

        List<Integer> result = plan.execute(
                List.of(0),
                batch -> {
                    List<Integer> children = new ArrayList<>();
                    for (int n : batch) {
                        int left = 2 * n + 1;
                        int right = 2 * n + 2;
                        if (left < (1 << depth) - 1) {
                            children.add(left);
                        }
                        if (right < (1 << depth) - 1) {
                            children.add(right);
                        }
                    }
                    return children;
                });

        // BFS: all nodes at level k appear before any node at level k+1
        assertThat(result).isNotEmpty();
        assertThat(result.get(0)).isEqualTo(0);
        // Node 1 and 2 are level 1; node 3 is level 2 (left child of node 1)
        assertThat(result.indexOf(1)).isLessThan(result.indexOf(3));
        assertThat(result.indexOf(2)).isLessThan(result.indexOf(3));
    }
}
