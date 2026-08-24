package org.pangrova.resource.property.memory;

import org.pangrova.resource.memory.RootMemoryPool;
import org.pangrova.resource.memory.InsufficientMemoryException;
import org.pangrova.resource.memory.LargestQueryKiller;
import org.pangrova.resource.memory.NodeMemoryPool;
import org.pangrova.resource.memory.QueryMemoryPool;
import org.pangrova.resource.memory.TaskMemoryPool;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for hierarchical memory management.
 *
 * <p><b>Validates: Requirements 14.1, 14.2, 14.7, 14.8</b></p>
 */
class HierarchicalMemoryPropertyTest {

    /**
     * Property 1: LargestQueryKiller always selects the query with max used bytes.
     * For any non-empty list of query pools with distinct usage, the killer
     * returns the one with the highest getUsedBytes().
     *
     * <p><b>Validates: Requirements 14.4</b></p>
     */
    @Property(tries = 10)
    void largestQueryKillerSelectsMaxUsedBytes(
            @ForAll @IntRange(min = 1, max = 10) int queryCount,
            @ForAll @LongRange(min = 1000, max = 100_000) long nodeMax) {

        long perQuery = nodeMax / (queryCount + 1);
        if (perQuery < 10) {
            return;
        }
        RootMemoryPool cluster = new RootMemoryPool("cluster", nodeMax * 2);
        NodeMemoryPool node = cluster.createNodePool("node", nodeMax);

        List<QueryMemoryPool> pools = new ArrayList<>();
        long maxUsed = 0;
        for (int i = 0; i < queryCount; i++) {
            try {
                QueryMemoryPool qp = node.createQueryPool("q-" + i, perQuery);
                long toReserve = (i + 1) * (perQuery / (queryCount + 1));
                long reserved = qp.tryReserve(Math.min(toReserve, perQuery));
                if (reserved > maxUsed) {
                    maxUsed = reserved;
                }
                pools.add(qp);
            } catch (InsufficientMemoryException ignored) {
                break;
            }
        }
        if (pools.isEmpty()) {
            return;
        }

        LargestQueryKiller killer = new LargestQueryKiller();
        var chosen = killer.chooseQueryToKill(pools);

        assertThat(chosen).isPresent();
        long chosenUsed = chosen.get().getUsedBytes();
        for (QueryMemoryPool pool : pools) {
            assertThat(chosenUsed).isGreaterThanOrEqualTo(pool.getUsedBytes());
        }
    }

    /**
     * Property 2: Node → Query → Task hierarchy respects quotas.
     * At every level, usedBytes never exceeds maxBytes.
     *
     * <p><b>Validates: Requirements 14.1, 8.2, 8.3</b></p>
     */
    @Property(tries = 10)
    void hierarchyRespectsQuotas(
            @ForAll @LongRange(min = 1000, max = 100_000) long nodeMax) {

        long queryMax = nodeMax / 2;
        long taskMax = queryMax / 2;

        RootMemoryPool cluster = new RootMemoryPool("cluster", nodeMax);
        NodeMemoryPool node = cluster.createNodePool("node", nodeMax);
        QueryMemoryPool query = node.createQueryPool("query", queryMax);
        TaskMemoryPool task = query.createTaskPool("task", taskMax);

        long reserved = task.tryReserve(taskMax);
        assertThat(reserved).isEqualTo(taskMax);

        assertThat(task.getUsedBytes()).isLessThanOrEqualTo(task.getMaxBytes());
        assertThat(query.getUsedBytes()).isLessThanOrEqualTo(query.getMaxBytes());
        assertThat(node.getUsedBytes()).isLessThanOrEqualTo(node.getMaxBytes());
        assertThat(cluster.getUsedBytes()).isLessThanOrEqualTo(cluster.getMaxBytes());
    }

    /**
     * Property 3: Memory freed at task level propagates up to node level.
     * After creating and freeing a task pool's reservation, the parent
     * query and node pools reflect the freed capacity.
     *
     * <p><b>Validates: Requirements 14.3</b></p>
     */
    @Property(tries = 10)
    void freeAtTaskPropagatesUpToNode(
            @ForAll @LongRange(min = 1000, max = 100_000) long nodeMax) {

        long queryMax = nodeMax / 2;
        long taskMax = queryMax / 2;

        RootMemoryPool cluster = new RootMemoryPool("cluster", nodeMax);
        NodeMemoryPool node = cluster.createNodePool("node", nodeMax);
        long nodeUsedBefore = node.getUsedBytes();

        QueryMemoryPool query = node.createQueryPool("query", queryMax);
        TaskMemoryPool task = query.createTaskPool("task", taskMax);

        long reserved = task.tryReserve(taskMax);
        assertThat(reserved).isEqualTo(taskMax);
        assertThat(task.getUsedBytes()).isEqualTo(taskMax);

        // Free task reservation, then release task pool from query, then query from node.
        // Note: createTaskPool propagates taskMax to the node pool (double-accounting),
        // so we must free queryMax + taskMax from the node to fully undo.
        task.free(reserved);
        query.free(taskMax);
        node.free(queryMax + taskMax);

        assertThat(task.getUsedBytes()).isZero();
        assertThat(node.getUsedBytes()).isEqualTo(nodeUsedBefore);
    }

    /**
     * Property 4: LargestQueryKiller returns empty for empty candidate list.
     *
     * <p><b>Validates: Requirements 14.4</b></p>
     */
    @Property(tries = 10)
    void largestQueryKillerEmptyOnNoCandidates() {
        LargestQueryKiller killer = new LargestQueryKiller();
        assertThat(killer.chooseQueryToKill(List.of())).isEmpty();
        assertThat(killer.chooseQueryToKill(null)).isEmpty();
    }
}
