package org.pangrova.metadata.relation;

import org.pangrova.metadata.api.relation.CascadePolicy;
import org.pangrova.metadata.api.relation.DependencyViolationException;
import org.pangrova.metadata.spi.dependency.MetadataMaintenanceHook;
import org.pangrova.persistence.catalog.api.dependency.ObjectDependencyStore;
import org.pangrova.metadata.api.relation.ObjectRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * High-level facade over {@link ObjectDependencyStore} that adds policy
 * enforcement and convenient hooks for the catalog DDL paths.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>Record dependencies on CREATE / REPLACE of a derived object
 *       (view, materialized view, dynamic table). Callers extract the
 *       referenced base objects from a parsed AST or planner state and
 *       pass them in as {@link ObjectRef}s.</li>
 *   <li>Enforce {@link CascadePolicy} on DROP of a parent: RESTRICT throws,
 *       CASCADE invokes a caller-supplied "drop one object" hook for each
 *       transitive dependent in topological order, WARN logs and proceeds.</li>
 *   <li>Clean up the dependency graph itself once a drop succeeds — both
 *       sides of every edge mentioning the dropped object are removed.</li>
 *   <li>Notify registered {@link MetadataMaintenanceHook}s when the
 *       dependency graph changes so external systems (lineage catalogs,
 *       maintenance schedulers) stay in sync.</li>
 * </ol>
 *
 * <p>The service is intentionally agnostic of how dependencies are extracted
 * from SQL — the parser-aware extraction lives in caller modules. This keeps
 * {@code pangrova-metadata-core} from depending on {@code pangrova-parser}.
 */
public final class ObjectDependencyService {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectDependencyService.class);

    private final ObjectDependencyStore store;
    private final List<MetadataMaintenanceHook> hooks = new CopyOnWriteArrayList<>();

    public ObjectDependencyService(ObjectDependencyStore store) {
        this.store = Objects.requireNonNull(store, "store must not be null");
    }

    /**
     * Registers a {@link MetadataMaintenanceHook} that will be notified on
     * dependency-graph mutations (object drops, dependency changes, snapshot
     * expiry). Hooks are invoked in registration order. Thread-safe.
     */
    public void addMaintenanceHook(MetadataMaintenanceHook hook) {
        Objects.requireNonNull(hook, "hook must not be null");
        hooks.add(hook);
        LOG.info("event=maintenance_hook_registered hook={}", hook.getClass().getSimpleName());
    }

    /**
     * Removes a previously registered maintenance hook. Thread-safe.
     *
     * @return true if the hook was found and removed
     */
    public boolean removeMaintenanceHook(MetadataMaintenanceHook hook) {
        return hooks.remove(hook);
    }

    /**
     * Convenience: records a single dependency edge.
     */
    public void recordDependency(ObjectRef child, ObjectRef parent) {
        recordDependencies(child, Set.of(parent));
    }

    /**
     * Records that {@code child} depends on each of {@code parents}.
     *
     * <p>Used on CREATE VIEW / CREATE MATERIALIZED VIEW / CREATE DYNAMIC
     * TABLE. Callers should prefer {@link #replaceDependencies} for
     * CREATE OR REPLACE semantics.
     */
    public void recordDependencies(ObjectRef child, Set<ObjectRef> parents) {
        Objects.requireNonNull(child, "child");
        Objects.requireNonNull(parents, "parents");
        if (parents.isEmpty()) {
            LOG.debug("event=record_dependencies_skipped reason=empty_parents child={}", child);
            return;
        }
        Set<ObjectRef> storedParents = store.recordDependencies(child, parents);
        LOG.info("event=dependencies_recorded child={} parents={}", child, parents);
        fireOnDependenciesChanged(child, storedParents);
    }

    /**
     * Atomic CREATE OR REPLACE of a child's parent set. Removes any edge from
     * a previously-referenced parent and inserts edges for {@code parents}.
     * Notification uses the storage operation's immutable result; no later read
     * is used to reconstruct the set after the write has already committed.
     */
    public void replaceDependencies(ObjectRef child, Set<ObjectRef> parents) {
        Objects.requireNonNull(child, "child");
        Objects.requireNonNull(parents, "parents");
        Set<ObjectRef> storedParents = store.replaceDependenciesForChild(child, parents);
        LOG.info("event=dependencies_replaced child={} parents={}", child, parents);
        fireOnDependenciesChanged(child, storedParents);
    }

    /**
     * Returns direct dependents of {@code parent}.
     */
    public List<ObjectRef> getDependents(ObjectRef parent) {
        return store.getDependentsOf(parent);
    }

    /**
     * Returns all parents of {@code child} (i.e. the lineage).
     */
    public List<ObjectRef> getParents(ObjectRef child) {
        return store.getParentsOf(child);
    }

    /**
     * Returns all transitive dependents in cascade-order (deepest first).
     */
    public List<ObjectRef> getTransitiveDependents(ObjectRef parent) {
        return store.getTransitiveDependentsOf(parent);
    }

    /**
     * Apply the {@link CascadePolicy} for an upcoming drop of {@code parent}.
     * MUST be called BEFORE the parent is removed from its connector — once
     * the parent is gone, listeners can race against the now-stale graph.
     *
     * <p>Behavior by policy:
     * <ul>
     *   <li>{@link CascadePolicy#RESTRICT}: if any direct dependent exists,
     *       throws {@link DependencyViolationException} listing them. Returns
     *       an empty list otherwise.</li>
     *   <li>{@link CascadePolicy#CASCADE}: invokes {@code dropper} for each
     *       transitive dependent in deepest-first order, then removes their
     *       edges and notifies hooks for that completed step. Returns the list
     *       of objects that were dropped. The dropper owns connector deletion;
     *       the service owns graph cleanup and per-object notification.</li>
     *   <li>{@link CascadePolicy#WARN}: logs a warning per dependent and returns
     *       the dependents that would be orphaned. The graph is unchanged until
     *       the caller confirms a successful parent drop with {@link #onObjectDropped}.</li>
     * </ul>
     *
     * <p>The {@code dropper} is supplied by the caller because actually
     * removing a view/table requires going through the connector, which
     * lives outside this module. The dropper is invoked inside this method
     * so a partial CASCADE failure surfaces immediately and the parent's
     * own drop can be aborted.
     *
     * @return list of objects the caller should consider broken / dropped
     *         (empty for RESTRICT when the parent is leaf)
     */
    public List<ObjectRef> applyDropPolicy(
            ObjectRef parent, CascadePolicy policy, Consumer<ObjectRef> dropper) {
        Objects.requireNonNull(parent, "parent");
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(dropper, "dropper");
        return switch (policy) {
            case RESTRICT -> applyRestrict(parent);
            case CASCADE -> applyCascade(parent, dropper);
            case WARN -> applyWarn(parent);
        };
    }

    /**
     * Removes every dependency edge mentioning {@code obj}. Called AFTER the
     * connector-side drop succeeds to keep the graph consistent.
     *
     * <p>Also notifies all registered {@link MetadataMaintenanceHook}s via
     * {@code onObjectDropped}.
     *
     * @return number of edges purged
     */
    public int onObjectDropped(ObjectRef obj) {
        return onObjectDropped(obj, List.of());
    }

    /**
     * Removes every dependency edge mentioning {@code obj}, recording the
     * set of cascade-dropped dependents for hook notification.
     *
     * @param obj                the dropped object
     * @param cascadedDependents objects that were cascade-dropped before
     *                           this one (empty if not a cascade drop)
     * @return number of edges purged
     */
    public int onObjectDropped(ObjectRef obj, List<ObjectRef> cascadedDependents) {
        Objects.requireNonNull(obj, "obj");
        List<ObjectRef> cascadeSnapshot = List.copyOf(cascadedDependents);
        int removed = store.removeAllEdgesFor(obj);
        if (removed > 0) {
            LOG.debug("event=dependency_edges_purged obj={} count={}", obj, removed);
        }
        fireOnObjectDropped(obj, cascadeSnapshot);
        return removed;
    }

    /**
     * Notifies all registered {@link MetadataMaintenanceHook}s that a
     * snapshot has been expired. Called by connector-level maintenance
     * procedures (e.g. Iceberg's {@code ExpireSnapshots}).
     *
     * @param ref        the table whose snapshot expired
     * @param snapshotId the expired snapshot identifier
     */
    public void notifySnapshotExpired(ObjectRef ref, long snapshotId) {
        Objects.requireNonNull(ref, "ref");
        LOG.info("event=snapshot_expired ref={} snapshotId={}", ref, snapshotId);
        for (MetadataMaintenanceHook hook : hooks) {
            try {
                hook.onSnapshotExpired(ref, snapshotId);
            } catch (RuntimeException e) {
                LOG.warn("event=maintenance_hook_failed hook={} method=onSnapshotExpired error={}",
                        hook.getClass().getSimpleName(), e.getMessage(), e);
            }
        }
    }

    private List<ObjectRef> applyRestrict(ObjectRef parent) {
        List<ObjectRef> dependents = store.getDependentsOf(parent);
        if (dependents.isEmpty()) {
            return List.of();
        }
        throw new DependencyViolationException(parent, dependents);
    }

    private List<ObjectRef> applyCascade(ObjectRef parent, Consumer<ObjectRef> dropper) {
        List<ObjectRef> dependents = store.getTransitiveDependentsOf(parent);
        if (dependents.isEmpty()) {
            return List.of();
        }
        // Deepest-first by contract from getTransitiveDependentsOf — drop in that order.
        for (ObjectRef dep : dependents) {
            try {
                dropper.accept(dep);
            } catch (RuntimeException e) {
                LOG.error("event=cascade_drop_failed parent={} dependent={} reason={}",
                        parent, dep, e.getMessage(), e);
                throw e;
            }
            onObjectDropped(dep);
            LOG.info("event=cascade_dropped parent={} dependent={}", parent, dep);
        }
        return dependents;
    }

    private List<ObjectRef> applyWarn(ObjectRef parent) {
        List<ObjectRef> dependents = store.getDependentsOf(parent);
        for (ObjectRef dep : dependents) {
            LOG.warn("event=dependency_orphaning_requested parent={} dependent={} "
                    + "reason=parent_drop_requested_with_WARN_policy", parent, dep);
        }
        // The connector has not dropped the parent yet. onObjectDropped owns cleanup.
        return dependents;
    }

    // ── Hook fire helpers ──

    private void fireOnObjectDropped(ObjectRef ref, List<ObjectRef> cascadedDependents) {
        for (MetadataMaintenanceHook hook : hooks) {
            try {
                hook.onObjectDropped(ref, cascadedDependents);
            } catch (RuntimeException e) {
                LOG.warn("event=maintenance_hook_failed hook={} method=onObjectDropped error={}",
                        hook.getClass().getSimpleName(), e.getMessage(), e);
            }
        }
    }

    private void fireOnDependenciesChanged(ObjectRef child, Set<ObjectRef> newParents) {
        for (MetadataMaintenanceHook hook : hooks) {
            try {
                hook.onDependenciesChanged(child, newParents);
            } catch (RuntimeException e) {
                LOG.warn("event=maintenance_hook_failed hook={} method=onDependenciesChanged error={}",
                        hook.getClass().getSimpleName(), e.getMessage(), e);
            }
        }
    }
}
