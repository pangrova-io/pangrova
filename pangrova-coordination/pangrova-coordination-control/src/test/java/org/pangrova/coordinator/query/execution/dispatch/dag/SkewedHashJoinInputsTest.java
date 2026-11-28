package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.stage.ShuffleInputLayout;
import org.pangrova.planner.adaptivequery.AdaptiveQueryConfig;
import org.pangrova.planner.analysis.PlanFragment.AlignedHashJoin;
import org.pangrova.planner.api.adaptivequery.CoalesceResult;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShuffleCommitReceipt.Partition;
import org.pangrova.shuffle.api.ShuffleCommitReceipt.ReadRange;
import org.pangrova.shuffle.api.ShufflePublication;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class SkewedHashJoinInputsTest {
    private static final String DIGEST = "00".repeat(32);
    private static final long MIB = 1024L * 1024L;
    private static final AlignedHashJoin INNER = new AlignedHashJoin("left", "right", true, true);

    @Test
    void rejectsTaskAndReadAmplificationWhenBothSidesAreHot() {
        var side = receipt(List.of(small(), certified(6, 64), small(), small()));
        assertThat(SkewedHashJoinInputs.select(INNER, side, side, CoalesceResult.NO_OP, AdaptiveQueryConfig.ENABLED))
                .isEqualTo(ShuffleInputLayout.DEFAULT);
    }

    @Test
    void reservesTheBoundedExpansionForTheLargestBucket() {
        var left = receipt(List.of(small(), certified(32, 64), small(), certified(32, 80), small()));
        var right = receipt(List.of(small(), small(), small(), small(), small()));
        var layout = SkewedHashJoinInputs.select(INNER, left, right, CoalesceResult.NO_OP, AdaptiveQueryConfig.ENABLED);

        assertThat(layout.consumers()).hasSize(36);
        assertThat(layout.consumers().stream().filter(consumer -> !consumer.ranges().isEmpty()))
                .hasSize(32).allSatisfy(consumer -> {
                    assertThat(consumer.partitions().startInclusive()).isEqualTo(3);
                    assertThat(consumer.ranges()).containsOnlyKeys("left");
                });
        assertThat(layout.consumers()).filteredOn(consumer -> consumer.partitions().startInclusive() == 1)
                .singleElement().satisfies(consumer -> assertThat(consumer.ranges()).isEmpty());
    }

    private static ShuffleCommitReceipt receipt(List<Partition> partitions) {
        var byId = new HashMap<Integer, Partition>();
        for (int id = 0; id < partitions.size(); id++) {
            byId.put(id, partitions.get(id));
        }
        return new ShuffleCommitReceipt(new ShufflePublication("query", 0, 1, "shuffle"), 1, DIGEST, byId);
    }

    private static Partition certified(int count, int intervalMib) {
        long bytes = intervalMib * MIB;
        return new Partition(count - 1L, count * bytes, count, DIGEST, IntStream.range(0, count)
                .mapToObj(id -> new ReadRange(id, id + 1L, bytes, 1, DIGEST)).toList());
    }

    private static Partition small() {
        return new Partition(0, MIB, 1, DIGEST);
    }
}
