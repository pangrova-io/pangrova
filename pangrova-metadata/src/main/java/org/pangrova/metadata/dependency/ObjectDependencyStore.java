package org.pangrova.metadata.dependency;

import java.util.List;
import java.util.Set;

/**
 * Persistent store for the catalog's object-dependency graph.
 *
 * <p>The graph is the ground truth for "which views depend on which tables,
 * which materialized views derive from which base tables, which dynamic
 * tables consume which streams." It is consulted on:
 * <ul>
 *   <li>DROP TABLE / DROP VIEW / DROP MATERIALIZED VIEW —
 *       to enforce {@link CascadePolicy} (RESTRICT vs CASCADE vs WARN);</li>
 *   <li>Lineage queries — {@code SHOW DEPENDENCIES OF x};</li>
 *   <li>Cross-object cache invalidation — invalidating a base table also
 *       evicts any cached MV plan that references it (future hook).</li>
 * </ul>
 *
 * <p>Edges are uniquely identified by {@code (parent_catalog, parent_schema,
 * parent_object, child_catalog, child_schema, child_object)} — kinds are stored
 * for the consumer's convenience but are NOT part of the identity, since an
 * object cannot change kind without a drop/recreate.
 *
 * <p>All methods MUST be thread-safe; implementations may serialize internally.
 */
public interface ObjectDependencyStore {

    /**
     * Records that {@code child} depends on every {@code parent} in the given
     * collection. The set is the new authoritative set of parents for this
     * child — any pre-existing edges from a different parent set are NOT
     * removed by this call (use {@link #replaceDependenciesForChild} for that).
     *
     * <p>Idempotent: re-recording an existing edge is a no-op (no exception,
     * no duplicate row). Implementations enforce uniqueness via the natural
     * primary key on {@code (parent..., child...)}.
     *
     * @param child   the dependent object (e.g. a view); must not be null
     * @param parents zero or more parent objects (e.g. base tables); may be empty
     */
    void recordDependencies(ObjectRef child, Set<ObjectRef> parents);

    /**
     * Atomically replaces the full set of parents for {@code child}:
     * deletes every existing edge whose child matches {@code child}, then
     * inserts an edge from each {@code parent} in the new set.
     *
     * <p>This is the natural primitive for "redefine VIEW v AS new_query" —
     * the view's dependency footprint changes wholesale, and a leftover edge
     * to a no-longer-referenced table would be a stale-state bug.
     */
    void replaceDependenciesForChild(ObjectRef child, Set<ObjectRef> parents);

    /**
     * Removes every edge mentioning the given object on either side
     * (i.e. all edges where {@code obj} is parent OR child). Called on DROP.
     *
     * @return the number of edges removed
     */
    int removeAllEdgesFor(ObjectRef obj);

    /**
     * Returns all parents of {@code child}: "what does {@code child} read?"
     * Used by lineage and snapshot-coverage planning.
     */
    List<ObjectRef> getParentsOf(ObjectRef child);

    /**
     * Returns all direct dependents of {@code parent}: "what reads from
     * {@code parent}?" Used by DROP enforcement and cascade planning.
     *
     * <p>Direct only — to compute transitive dependents, traverse the graph
     * via repeated calls.
     */
    List<ObjectRef> getDependentsOf(ObjectRef parent);

    /**
     * Returns all direct AND transitive dependents of {@code parent}, in
     * topological order suitable for cascade-drop (deepest descendants first,
     * direct dependents last).
     *
     * <p>Implementations must guard against cycles: a malformed graph with a
     * cycle yields the dependents discovered up to the first revisit, without
     * throwing.
     */
    List<ObjectRef> getTransitiveDependentsOf(ObjectRef parent);

    /**
     * Returns true iff {@code obj} has at least one direct dependent.
     */
    default boolean hasDependents(ObjectRef parent) {
        return !getDependentsOf(parent).isEmpty();
    }
}
