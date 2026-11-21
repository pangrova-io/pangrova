package org.pangrova.coordinator.query.task;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.task.OperatorStats;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueryOperatorStatsRegistryTest {

    private static OperatorStats stats(String type, String node, long rows, long peakMemory) {
        return new OperatorStats(type, 0, rows, 0, 0, 0, 0, node, 0, peakMemory, 0);
    }

    @Test
    void latestCumulativeReportReplacesEarlierOneForTheSameTaskAndNode() {
        var registry = new QueryOperatorStatsRegistry();
        registry.record("q-1", "task-a", List.of(stats("TableScan", "node-1", 10, 100)));
        registry.record("q-1", "task-a", List.of(stats("TableScan", "node-1", 40, 250)));

        assertThat(registry.operatorStats("q-1"))
                .singleElement()
                .satisfies(metric -> {
                    assertThat(metric.outputRows()).isEqualTo(40);
                    assertThat(metric.peakMemoryBytes()).isEqualTo(250);
                });
    }

    @Test
    void aggregatesAcrossTasksAndKeepsMaximumPeakMemory() {
        var registry = new QueryOperatorStatsRegistry();
        registry.record("q-2", "task-a", List.of(stats("Filter", "node-2", 10, 100)));
        registry.record("q-2", "task-b", List.of(stats("Filter", "node-2", 5, 400)));

        assertThat(registry.operatorStats("q-2"))
                .singleElement()
                .satisfies(metric -> {
                    assertThat(metric.operatorType()).isEqualTo("Filter");
                    assertThat(metric.planNodeId()).isEqualTo("node-2");
                    assertThat(metric.outputRows()).isEqualTo(15);
                    assertThat(metric.peakMemoryBytes()).isEqualTo(400);
                });
    }

    @Test
    void reportsWithoutPlanNodeIdentityGroupByOperatorType() {
        var registry = new QueryOperatorStatsRegistry();
        registry.record("q-3", "task-a", List.of(stats("Project", "", 3, 0)));
        registry.record("q-3", "task-b", List.of(stats("Project", null, 4, 0)));

        assertThat(registry.operatorStats("q-3"))
                .singleElement()
                .satisfies(metric -> {
                    assertThat(metric.planNodeId()).isEqualTo("Project");
                    assertThat(metric.outputRows()).isEqualTo(7);
                });
    }

    @Test
    void unknownOrEmptyQueriesReturnNothing() {
        var registry = new QueryOperatorStatsRegistry();
        assertThat(registry.operatorStats("absent")).isEmpty();
        registry.record("q-4", "task-a", List.of());
        assertThat(registry.operatorStats("q-4")).isEmpty();
    }
}
