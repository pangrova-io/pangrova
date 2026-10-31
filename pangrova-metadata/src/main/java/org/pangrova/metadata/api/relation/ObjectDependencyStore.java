package org.pangrova.metadata.api.relation;

import java.util.List;
import java.util.Set;

/**
 * Persistent store for the catalog's object-dependency graph.
 *
 * <p>This is an explicit dependency-graph component for callers that record
 * and maintain their own edges. Current SQL DROP and materialization lifecycles
 * do not use this store; their authority remains in their respective domains.
 * The key has no tenant or object-incarnation dimension, so it must not be wired
 * as a cross-tenant SQL dependency authority without a separate identity and
 * migration design.
 *
 * <p>Edges are uniquely identified by {@code (parent_catalog, parent_schema,
 * parent_object, child_catalog, child_schema, child_object)} — kinds are stored
 * as per-edge metadata but are NOT part of graph identity. Lookups, removal,
 * self-edge rejection and traversal use the natural name regardless of the
 * kind carried by the caller. This graph does not establish current object
 * existence or kind; that authority belongs to the owning catalog.
 *
 * <p>Mutation snapshots describe that operation's observation, not a durable
 * event or a guarantee that no concurrent writer subsequently changes the set.
 * Snapshot acquisition failure aborts the mutation; callers need no post-commit
 * read to construct a notification. JDBC commit acknowledgement or connection
 * close failure can still leave the usual indeterminate outcome.
 *
 * <p>All methods MUST be thread-safe; implementations may serialize internally.
 */
public interface ObjectDependencyStore {

    /**
     * Records that {@code child} depends on every {@code parent} in the given
     * collection. This adds edges: pre-existing edges from other parents are
     * not removed by this call (use {@link #replaceDependenciesForChild} for that).
     *
     * <p>Re-recording an existing natural edge does not duplicate it; its
     * parent/child kind descriptions are refreshed from this call. Other
     * edges are unchanged. A natural self-edge is ignored even if its two
     * references carry different kinds. Implementations validate all parent
     * references before mutating the graph.
     *
     * @param child   the dependent object (e.g. a view); must not be null
     * @param parents zero or more non-null parent objects; may be empty
     * @return immutable complete parent set observed after the mutation, under
     *         the same lock or before committing the same transaction; an empty
     *         input returns the existing set without changing edges
     */
    Set<ObjectRef> recordDependencies(ObjectRef child, Set<ObjectRef> parents);

    /**
     * Atomically replaces the full set of parents for {@code child}:
     * deletes every existing edge whose child matches {@code child}, then
     * inserts an edge from each {@code parent} in the new set.
     *
     * <p>This is the natural primitive for "redefine VIEW v AS new_query" —
     * the view's dependency footprint changes wholesale, and a leftover edge
     * to a no-longer-referenced table would be a stale-state bug.
     * Parent validation, kind refresh and self-edge handling follow
     * {@link #recordDependencies}; invalid input leaves existing edges intact.
     *
     * @return immutable complete parent set observed under the mutation's lock
     *         or transaction, with ignored self-edges omitted
     */
    Set<ObjectRef> replaceDependenciesForChild(ObjectRef child, Set<ObjectRef> parents);

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
     * <p>Each natural object identity is visited once and the root identity is
     * excluded, regardless of per-edge kind descriptions. The returned reference
     * carries the first discovered edge's kind, not a current catalog lookup. A
     * malformed cycle terminates without throwing or duplicating objects;
     * topological ordering is only defined for an acyclic graph.
     */
    List<ObjectRef> getTransitiveDependentsOf(ObjectRef parent);

    /**
     * Returns true iff {@code obj} has at least one direct dependent.
     */
    default boolean hasDependents(ObjectRef parent) {
        return !getDependentsOf(parent).isEmpty();
    }
}
