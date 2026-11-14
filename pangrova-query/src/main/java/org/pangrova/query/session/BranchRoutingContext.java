package org.pangrova.query.session;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Stores per-table branch routing mappings within a session.
 *
 * <p>When a user executes {@code USE BRANCH name ON table}, the mapping is stored
 * here. Subsequent queries resolve the table to the specified branch version.
 * {@code USE BRANCH MAIN ON table} removes the routing (reverts to main branch).</p>
 *
 * <p>Branch routing state propagates to Workers via session properties using the
 * key prefix {@value #PROPERTY_PREFIX}. Each entry is serialized as
 * {@code branch.routing.<qualified_table_name> = <branch_name>}.</p>
 *
 * <p>This class is immutable. Mutation methods return new instances.</p>
 */
public final class BranchRoutingContext {

    /**
     * Session property key prefix for branch routing entries.
     * Full key format: {@code branch.routing.CATALOG.SCHEMA.TABLE}
     */
    static final String PROPERTY_PREFIX = org.pangrova.common.client.query.BranchSelection.PROPERTY_PREFIX;

    private static final BranchRoutingContext EMPTY = new BranchRoutingContext(Map.of());

    private final Map<String, String> tableToBranch;

    private BranchRoutingContext(Map<String, String> tableToBranch) {
        this.tableToBranch = Map.copyOf(tableToBranch);
    }

    /**
     * Returns an empty context with no branch routing.
     */
    public static BranchRoutingContext empty() {
        return EMPTY;
    }

    /**
     * Extracts branch routing state from session properties.
     *
     * @param sessionProperties the full session properties map
     * @return a context containing all branch routing entries found
     */
    public static BranchRoutingContext fromSessionProperties(Map<String, String> sessionProperties) {
        if (sessionProperties == null || sessionProperties.isEmpty()) {
            return EMPTY;
        }
        Map<String, String> routes = new HashMap<>();
        for (Map.Entry<String, String> entry : sessionProperties.entrySet()) {
            if (entry.getKey().startsWith(PROPERTY_PREFIX)) {
                String qualifiedTable = entry.getKey().substring(PROPERTY_PREFIX.length());
                routes.put(qualifiedTable, entry.getValue());
            }
        }
        return routes.isEmpty() ? EMPTY : new BranchRoutingContext(routes);
    }

    /**
     * Returns a new context with the specified table routed to the given branch.
     *
     * @param qualifiedTableName fully qualified table name (catalog.schema.table)
     * @param branchName         the branch to route to
     * @return a new context with the routing added
     */
    public BranchRoutingContext withBranch(String qualifiedTableName, String branchName) {
        Map<String, String> updated = new HashMap<>(tableToBranch);
        updated.put(qualifiedTableName, branchName);
        return new BranchRoutingContext(updated);
    }

    /**
     * Returns a new context with the branch routing removed for the specified table,
     * reverting it to the main branch.
     *
     * @param qualifiedTableName fully qualified table name (catalog.schema.table)
     * @return a new context without the routing for that table
     */
    public BranchRoutingContext withoutBranch(String qualifiedTableName) {
        if (!tableToBranch.containsKey(qualifiedTableName)) {
            return this;
        }
        Map<String, String> updated = new HashMap<>(tableToBranch);
        updated.remove(qualifiedTableName);
        return updated.isEmpty() ? EMPTY : new BranchRoutingContext(updated);
    }

    /**
     * Returns the branch name for the given table, if routed.
     *
     * @param qualifiedTableName fully qualified table name (catalog.schema.table)
     * @return the branch name, or empty if the table uses the main branch
     */
    public Optional<String> getBranch(String qualifiedTableName) {
        return Optional.ofNullable(tableToBranch.get(qualifiedTableName));
    }

    /**
     * Returns true if any branch routing is active.
     */
    public boolean hasRouting() {
        return !tableToBranch.isEmpty();
    }

    /**
     * Returns all branch routing entries as an unmodifiable map.
     * Keys are qualified table names, values are branch names.
     */
    public Map<String, String> allRoutings() {
        return tableToBranch;
    }

    /**
     * Serializes branch routing state into session properties format.
     * The returned map contains only the branch routing entries (prefixed keys).
     *
     * @return session properties representing the branch routing state
     */
    public Map<String, String> toSessionProperties() {
        if (tableToBranch.isEmpty()) {
            return Map.of();
        }
        Map<String, String> props = new HashMap<>(tableToBranch.size());
        for (Map.Entry<String, String> entry : tableToBranch.entrySet()) {
            props.put(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(props);
    }

    /**
     * Merges branch routing properties into an existing session properties map.
     * Returns a new map containing all original properties plus branch routing entries.
     * Existing branch routing entries in the original map are replaced.
     *
     * @param originalProperties the original session properties
     * @return merged properties including branch routing state
     */
    public Map<String, String> mergeIntoSessionProperties(Map<String, String> originalProperties) {
        if (tableToBranch.isEmpty() && !hasBranchProperties(originalProperties)) {
            return originalProperties;
        }
        Map<String, String> merged = new HashMap<>();
        if (originalProperties != null) {
            for (Map.Entry<String, String> entry : originalProperties.entrySet()) {
                if (!entry.getKey().startsWith(PROPERTY_PREFIX)) {
                    merged.put(entry.getKey(), entry.getValue());
                }
            }
        }
        merged.putAll(toSessionProperties());
        return Collections.unmodifiableMap(merged);
    }

    private static boolean hasBranchProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return false;
        }
        for (String key : properties.keySet()) {
            if (key.startsWith(PROPERTY_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BranchRoutingContext that)) return false;
        return tableToBranch.equals(that.tableToBranch);
    }

    @Override
    public int hashCode() {
        return tableToBranch.hashCode();
    }

    @Override
    public String toString() {
        return "BranchRoutingContext" + tableToBranch;
    }
}
