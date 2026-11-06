package org.pangrova.planner.core.branch;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds per-table branch routing state for a session.
 *
 * <p>When USE BRANCH is active for a table, the planner resolves that table
 * to the branch version. Each table can be independently routed to a different
 * branch within the same session.</p>
 *
 * <p>Thread-safe: multiple queries in the same session may read routing state
 * concurrently while USE BRANCH statements modify it.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
public final class BranchRoutingContext {

    private final ConcurrentHashMap<String, String> tableRoutings;

    public BranchRoutingContext() {
        this.tableRoutings = new ConcurrentHashMap<>();
    }

    /**
     * Sets branch routing for a table. The table name is stored in lowercase
     * dot-separated form (e.g., "catalog.schema.table").
     *
     * @param tableFqn fully qualified table name (dot-separated, case-insensitive)
     * @param branchName branch name to route to; "main" removes routing
     */
    public void setRouting(String tableFqn, String branchName) {
        String key = tableFqn.toLowerCase();
        if ("main".equalsIgnoreCase(branchName)) {
            tableRoutings.remove(key);
        } else {
            tableRoutings.put(key, branchName);
        }
    }

    /**
     * Returns the active branch for a table, or empty if the table reads from main.
     *
     * @param tableFqn fully qualified table name (dot-separated, case-insensitive)
     * @return branch name if routing is active, empty otherwise
     */
    public Optional<String> getRouting(String tableFqn) {
        return Optional.ofNullable(tableRoutings.get(tableFqn.toLowerCase()));
    }

    /**
     * Returns an immutable snapshot of all active routings.
     * Used for propagating routing state to workers.
     */
    public Map<String, String> snapshot() {
        return Map.copyOf(tableRoutings);
    }

    /**
     * Returns true if any table has active branch routing.
     */
    public boolean hasActiveRoutings() {
        return !tableRoutings.isEmpty();
    }

    /**
     * Removes all active routings (session reset).
     */
    public void clear() {
        tableRoutings.clear();
    }
}
