package org.pangrova.worker.resource.memory;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LowMemoryKiller} strategy interface.
 */
class LowMemoryKillerTest {

    /**
     * A simple strategy that kills the query with the highest memory usage.
     */
    private static final LowMemoryKiller LARGEST_FIRST = candidates ->
            candidates.stream()
                    .max(Comparator.comparingLong(MemoryPool::getUsedBytes));

    @Test
    void selectsLargestConsumer() {
        var cluster = new RootMemoryPool("root", 10_000);
        NodeMemoryPool node = cluster.createNodePool("node-1", 10_000);

        QueryMemoryPool q1 = node.createQueryPool("q-1", 3000);
        q1.tryReserve(1000);

        QueryMemoryPool q2 = node.createQueryPool("q-2", 3000);
        q2.tryReserve(2500);

        QueryMemoryPool q3 = node.createQueryPool("q-3", 3000);
        q3.tryReserve(500);

        Optional<QueryMemoryPool> victim = LARGEST_FIRST.chooseQueryToKill(List.of(q1, q2, q3));
        assertThat(victim).isPresent().containsSame(q2);
    }

    @Test
    void returnsEmptyForEmptyCandidates() {
        Optional<QueryMemoryPool> victim = LARGEST_FIRST.chooseQueryToKill(List.of());
        assertThat(victim).isEmpty();
    }
}
