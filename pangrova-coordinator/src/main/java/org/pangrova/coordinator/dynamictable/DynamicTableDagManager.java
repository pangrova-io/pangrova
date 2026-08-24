package org.pangrova.coordinator.dynamictable;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.dynamictable.refresh.DagProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the dependency DAG for Dynamic Tables.
 *
 * <p>Tracks upstream → downstream edges to support cascade refresh,
 * cycle detection, topological ordering, and cascade removal.</p>
 *
 * <p>Concurrency strategy: all edge mutations use
 * {@link ConcurrentHashMap#compute} for atomicity. Read operations
 * return defensive copies.</p>
 */
@ThreadSafe
public final class DynamicTableDagManager implements DagProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableDagManager.class);

    // upstream → Set<downstream>
    private final ConcurrentHashMap<String, Set<String>> edges = new ConcurrentHashMap<>();
    // downstream → Set<upstream>
    private final ConcurrentHashMap<String, Set<String>> reverseEdges = new ConcurrentHashMap<>();

    /**
     * Register dependencies for a Dynamic Table.
     *
     * @param dtName       the DT being registered
     * @param sourceTables set of upstream table/DT names this DT depends on
     * @throws IllegalArgumentException if adding these edges would create a cycle
     */
    public void registerDependencies(String dtName, Set<String> sourceTables) {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(sourceTables, "sourceTables must not be null");
        validateNoCycle(dtName, sourceTables);
        addEdges(dtName, sourceTables);
        LOG.info("event=dt_dag_register dtName={} sources={}", dtName, sourceTables);
    }

    /**
     * DFS cycle detection. Throws if adding dtName with given upstreams would create a cycle.
     */
    public void validateNoCycle(String newNode, Set<String> upstreams) {
        for (String upstream : upstreams) {
            if (upstream.equals(newNode)) {
                throw new IllegalArgumentException(
                        "Cycle detected: " + newNode + " depends on itself");
            }
            Set<String> visited = new HashSet<>();
            if (canReach(newNode, upstream, visited)) {
                throw new IllegalArgumentException(
                        "Cycle detected: adding " + newNode + " → " + upstream
                                + " would create cycle via path: " + visited);
            }
        }
    }

    /** Return direct downstream DT names. */
    @Override
    public Set<String> getDirectDownstreams(String dtName) {
        Set<String> ds = edges.get(dtName);
        return ds == null ? Set.of() : Set.copyOf(ds);
    }

    /** Return all direct and indirect downstream DT names (transitive closure). */
    public Set<String> getDependents(String dtName) {
        Set<String> result = new LinkedHashSet<>();
        collectDependents(dtName, result);
        return Collections.unmodifiableSet(result);
    }

    /** Kahn's algorithm topological sort. Returns DT names in dependency order. */
    public List<String> topologicalOrder() {
        Set<String> allNodes = collectAllNodes();
        Map<String, Integer> inDegree = computeInDegree(allNodes);
        return kahnSort(allNodes, inDegree);
    }

    /** Remove a node and all its transitive downstreams. Returns removed nodes. */
    public Set<String> removeWithCascade(String dtName) {
        Set<String> toRemove = new LinkedHashSet<>();
        toRemove.add(dtName);
        collectDependents(dtName, toRemove);
        List<String> ordered = new ArrayList<>(toRemove);
        Collections.reverse(ordered);
        for (String node : ordered) {
            removeNode(node);
        }
        LOG.info("event=dt_dag_cascade_remove root={} removed={}", dtName, toRemove);
        return toRemove;
    }

    /** Rebuild DAG from persisted dependency map. */
    public void recover(Map<String, Set<String>> dependencyMap) {
        edges.clear();
        reverseEdges.clear();
        for (var entry : dependencyMap.entrySet()) {
            addEdges(entry.getKey(), entry.getValue());
        }
        LOG.info("event=dt_dag_recovered nodeCount={}", collectAllNodes().size());
    }

    /** Return the depth of a node in the DAG (0 = root/no upstreams). */
    public int getDepth(String dtName) {
        Set<String> upstreams = reverseEdges.get(dtName);
        if (upstreams == null || upstreams.isEmpty()) {
            return 0;
        }
        return computeDepth(dtName, new HashMap<>());
    }

    /**
     * Update dependencies for an existing Dynamic Table (e.g. after REDEFINE).
     *
     * <p>Removes old upstream edges for {@code dtName} and registers new ones.
     * Does NOT cascade-remove downstream dependents — only the node's own
     * upstream edges are replaced.</p>
     *
     * @param dtName          the DT being updated
     * @param newSourceTables new set of upstream table/DT names
     * @throws IllegalArgumentException if the new edges would create a cycle
     */
    public void updateDependencies(String dtName, Set<String> newSourceTables) {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(newSourceTables, "newSourceTables must not be null");
        validateNoCycle(dtName, newSourceTables);
        removeReverseEdges(dtName);
        addEdges(dtName, newSourceTables);
        LOG.info("event=dt_dag_update dtName={} newSources={}", dtName, newSourceTables);
    }

    // ── package-private for testing ─────────────────────────────────

    int nodeCount() {
        return collectAllNodes().size();
    }

    int edgeCount() {
        return edges.values().stream().mapToInt(Set::size).sum();
    }

    // ── Private helpers ─────────────────────────────────────────────

    private boolean canReach(String from, String target, Set<String> visited) {
        Set<String> downstreams = edges.get(from);
        if (downstreams == null) {
            return false;
        }
        for (String ds : downstreams) {
            if (!visited.add(ds)) {
                continue;
            }
            if (ds.equals(target) || canReach(ds, target, visited)) {
                return true;
            }
        }
        return false;
    }

    private void collectDependents(String dtName, Set<String> result) {
        Set<String> downstreams = edges.get(dtName);
        if (downstreams == null) {
            return;
        }
        for (String ds : downstreams) {
            if (result.add(ds)) {
                collectDependents(ds, result);
            }
        }
    }

    private void addEdges(String dtName, Set<String> sourceTables) {
        for (String source : sourceTables) {
            edges.computeIfAbsent(source, k -> ConcurrentHashMap.newKeySet()).add(dtName);
            reverseEdges.computeIfAbsent(dtName, k -> ConcurrentHashMap.newKeySet()).add(source);
        }
    }

    private void removeNode(String node) {
        removeForwardEdges(node);
        removeReverseEdges(node);
    }

    private void removeForwardEdges(String node) {
        Set<String> downstreams = edges.remove(node);
        if (downstreams == null) {
            return;
        }
        for (String ds : downstreams) {
            Set<String> rev = reverseEdges.get(ds);
            if (rev != null) {
                rev.remove(node);
            }
        }
    }

    private void removeReverseEdges(String node) {
        Set<String> upstreams = reverseEdges.remove(node);
        if (upstreams == null) {
            return;
        }
        for (String us : upstreams) {
            Set<String> fwd = edges.get(us);
            if (fwd != null) {
                fwd.remove(node);
            }
        }
    }

    private Set<String> collectAllNodes() {
        Set<String> all = new HashSet<>();
        all.addAll(edges.keySet());
        all.addAll(reverseEdges.keySet());
        edges.values().forEach(all::addAll);
        reverseEdges.values().forEach(all::addAll);
        return all;
    }

    private Map<String, Integer> computeInDegree(Set<String> allNodes) {
        Map<String, Integer> inDegree = new HashMap<>();
        for (String node : allNodes) {
            inDegree.put(node, 0);
        }
        for (var entry : edges.entrySet()) {
            for (String ds : entry.getValue()) {
                inDegree.merge(ds, 1, (a, b) -> a + b);
            }
        }
        return inDegree;
    }

    private List<String> kahnSort(Set<String> allNodes, Map<String, Integer> inDegree) {
        Deque<String> queue = seedQueue(inDegree);
        return drainQueue(queue, inDegree);
    }

    private static Deque<String> seedQueue(Map<String, Integer> inDegree) {
        Deque<String> queue = new ArrayDeque<>();
        for (var entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }
        return queue;
    }

    private List<String> drainQueue(Deque<String> queue, Map<String, Integer> inDegree) {
        List<String> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            String node = queue.poll();
            result.add(node);
            Set<String> downstreams = edges.get(node);
            if (downstreams == null) {
                continue;
            }
            for (String ds : downstreams) {
                int newDegree = inDegree.merge(ds, -1, (a, b) -> a + b);
                if (newDegree == 0) {
                    queue.add(ds);
                }
            }
        }
        return result;
    }

    private int computeDepth(String dtName, Map<String, Integer> memo) {
        if (memo.containsKey(dtName)) {
            return memo.get(dtName);
        }
        Set<String> upstreams = reverseEdges.get(dtName);
        if (upstreams == null || upstreams.isEmpty()) {
            memo.put(dtName, 0);
            return 0;
        }
        int maxDepth = 0;
        for (String upstream : upstreams) {
            maxDepth = Math.max(maxDepth, computeDepth(upstream, memo) + 1);
        }
        memo.put(dtName, maxDepth);
        return maxDepth;
    }
}
