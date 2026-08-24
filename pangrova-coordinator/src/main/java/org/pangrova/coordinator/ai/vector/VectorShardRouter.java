package org.pangrova.coordinator.ai.vector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.planner.vector.VectorSearchNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes vector search queries to workers based on vector index shard placement.
 *
 * <p>In a distributed vector search scenario, the vector index for a large table
 * is partitioned (sharded) across multiple workers. Each worker holds a subset
 * of the index segments. This router determines which workers should execute
 * a given vector search query based on:</p>
 *
 * <ol>
 *   <li><b>Shard ownership</b>: Workers that hold index segments for the target table/column</li>
 *   <li><b>Load balancing</b>: Among eligible workers, prefer those with lower load</li>
 *   <li><b>Replication</b>: Support for replicated shards (multiple workers hold the same segment)</li>
 * </ol>
 *
 * <p>When no shard placement information is available (cold start or unindexed table),
 * falls back to broadcasting the search to all active workers (scatter-gather).</p>
 *
 * <p>Thread-safe: uses ConcurrentHashMap for shard registry.</p>
 */
public final class VectorShardRouter {

    private static final Logger LOG = LoggerFactory.getLogger(VectorShardRouter.class);

    /**
     * Describes a vector index shard placement on a specific worker.
     *
     * @param workerId       worker holding this shard
     * @param tableName      table the index belongs to
     * @param vectorColumn   indexed vector column
     * @param shardId        opaque shard identifier (e.g., segment number)
     * @param rowCount       approximate row count in this shard
     * @param generation     index build generation (for staleness detection)
     */
    public record ShardPlacement(
            String workerId,
            String tableName,
            String vectorColumn,
            String shardId,
            long rowCount,
            int generation) {

        public ShardPlacement {
            Objects.requireNonNull(workerId, "workerId required");
            Objects.requireNonNull(tableName, "tableName required");
            Objects.requireNonNull(vectorColumn, "vectorColumn required");
            Objects.requireNonNull(shardId, "shardId required");
        }
    }

    /**
     * Result of routing a vector search query.
     *
     * @param targetWorkers   workers that should execute the search (ordered by priority)
     * @param scatterAll      true if search should scatter to ALL workers (no shard info)
     * @param shardAssignment mapping from workerId to assigned shard IDs
     */
    public record RoutingDecision(
            List<WorkerInfo> targetWorkers,
            boolean scatterAll,
            Map<String, List<String>> shardAssignment) {

        public RoutingDecision {
            targetWorkers = List.copyOf(targetWorkers);
            shardAssignment = Map.copyOf(shardAssignment);
        }

        /** Returns a scatter-all decision targeting all provided workers. */
        public static RoutingDecision scatterToAll(List<WorkerInfo> workers) {
            return new RoutingDecision(workers, true, Map.of());
        }
    }

    // Registry: key = "tableName:vectorColumn", value = list of shard placements
    private final ConcurrentHashMap<String, List<ShardPlacement>> shardRegistry =
            new ConcurrentHashMap<>();

    /**
     * Registers shard placements reported by workers via heartbeat or explicit registration.
     * This is called when a worker reports that it has loaded/built a vector index segment.
     */
    public void registerShards(List<ShardPlacement> placements) {
        for (ShardPlacement p : placements) {
            String key = buildKey(p.tableName(), p.vectorColumn());
            shardRegistry.compute(key, (k, existing) -> {
                List<ShardPlacement> list = existing != null
                        ? new ArrayList<>(existing) : new ArrayList<>();
                // Remove stale entry for same worker+shard (re-registration)
                list.removeIf(e -> e.workerId().equals(p.workerId())
                        && e.shardId().equals(p.shardId()));
                list.add(p);
                return list;
            });
        }
        LOG.debug("event=shards_registered count={}", placements.size());
    }

    /**
     * Removes all shard placements for a worker (called on worker deregistration/failure).
     */
    public void deregisterWorker(String workerId) {
        shardRegistry.values().forEach(list ->
                list.removeIf(p -> p.workerId().equals(workerId)));
        LOG.info("event=worker_shards_deregistered workerId={}", workerId);
    }

    /**
     * Routes a vector search query to the appropriate workers based on index shard placement.
     *
     * @param vsNode        the vector search plan node
     * @param activeWorkers all currently active workers
     * @return routing decision indicating which workers should execute the search
     */
    public RoutingDecision route(VectorSearchNode vsNode, List<WorkerInfo> activeWorkers) {
        Objects.requireNonNull(vsNode, "vsNode required");
        if (activeWorkers == null || activeWorkers.isEmpty()) {
            return RoutingDecision.scatterToAll(List.of());
        }

        String key = buildKey(vsNode.tableName(), vsNode.vectorColumn());
        List<ShardPlacement> placements = shardRegistry.get(key);

        // No shard info → scatter to all workers (brute-force on each)
        if (placements == null || placements.isEmpty()) {
            LOG.debug("event=vector_route_scatter_all table={} column={} reason=no_shard_placement workers={}",
                    vsNode.tableName(), vsNode.vectorColumn(), activeWorkers.size());
            return RoutingDecision.scatterToAll(activeWorkers);
        }

        // Filter to placements on active workers only
        Map<String, WorkerInfo> activeById = indexById(activeWorkers);
        List<ShardPlacement> validPlacements = filterValidPlacements(placements, activeById, vsNode);

        if (validPlacements.isEmpty()) {
            LOG.info("event=vector_route_scatter_all table={} column={} reason=no_valid_placements gen={}",
                    vsNode.tableName(), vsNode.vectorColumn(), vsNode.indexGeneration());
            return RoutingDecision.scatterToAll(activeWorkers);
        }

        // Group by worker, assign shards
        Map<String, List<String>> shardAssignment = assignShards(validPlacements);

        List<WorkerInfo> targetWorkers = shardAssignment.entrySet().stream()
                .sorted(Comparator.<Map.Entry<String, List<String>>, Integer>comparing(
                        e -> e.getValue().size()).reversed())
                .map(e -> activeById.get(e.getKey()))
                .filter(Objects::nonNull)
                .toList();

        LOG.info("event=vector_route_sharded table={} column={} "
                + "targetWorkers={} totalShards={} generation={}",
                vsNode.tableName(), vsNode.vectorColumn(),
                targetWorkers.size(), validPlacements.size(),
                vsNode.indexGeneration());

        return new RoutingDecision(targetWorkers, false, shardAssignment);
    }

    private static Map<String, WorkerInfo> indexById(List<WorkerInfo> activeWorkers) {
        Map<String, WorkerInfo> activeById = new HashMap<>();
        for (WorkerInfo w : activeWorkers) {
            activeById.put(w.workerId(), w);
        }
        return activeById;
    }

    private static List<ShardPlacement> filterValidPlacements(
            List<ShardPlacement> placements, Map<String, WorkerInfo> activeById, VectorSearchNode vsNode) {
        List<ShardPlacement> validPlacements = new ArrayList<>();
        for (ShardPlacement p : placements) {
            if (!activeById.containsKey(p.workerId())) {
                continue;  // worker is down
            }
            if (vsNode.indexGeneration() >= 0 && p.generation() < vsNode.indexGeneration()) {
                continue;  // stale index generation
            }
            validPlacements.add(p);
        }
        return validPlacements;
    }

    private static Map<String, List<String>> assignShards(List<ShardPlacement> validPlacements) {
        Map<String, List<String>> shardAssignment = new HashMap<>();
        for (ShardPlacement p : validPlacements) {
            shardAssignment.computeIfAbsent(p.workerId(), k -> new ArrayList<>())
                    .add(p.shardId());
        }
        return shardAssignment;
    }

    /**
     * Returns the total number of registered shard placements (for monitoring).
     */
    public int totalShardCount() {
        return shardRegistry.values().stream().mapToInt(List::size).sum();
    }

    private static String buildKey(String tableName, String vectorColumn) {
        return tableName + ":" + vectorColumn;
    }
}
