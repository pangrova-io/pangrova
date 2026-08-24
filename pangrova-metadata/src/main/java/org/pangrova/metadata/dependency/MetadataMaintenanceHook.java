package org.pangrova.metadata.dependency;

import java.util.List;
import java.util.Set;

/**
 * Callback interface for metadata maintenance operations triggered by
 * dependency-graph mutations (drops, dependency changes, snapshot expiry).
 *
 * <p>Implementations are registered on {@link ObjectDependencyService} and
 * invoked synchronously after the graph mutation succeeds. Hooks run on
 * the caller's thread; long-running work (e.g. external lineage transport)
 * should be offloaded to an async emitter such as
 * {@code LineageEventEmitter}.
 *
 * <p>All methods have empty defaults so callers can implement only the
 * callbacks they care about.
 *
 * <p>Example hook: a lineage bridge that emits an OpenLineage event when
 * an object is dropped, so external catalogs (Marquez, DataHub) stay in
 * sync with Pangrova's dependency graph.
 */
public interface MetadataMaintenanceHook {

    /**
     * Invoked after an object has been dropped and its dependency edges
     * purged from the graph.
     *
     * @param ref                the dropped object
     * @param cascadedDependents objects that were cascade-dropped before
     *                           this object (deepest-first order); empty
     *                           if the drop was a leaf or CASCADE was not
     *                           triggered
     */
    default void onObjectDropped(ObjectRef ref, List<ObjectRef> cascadedDependents) {}

    /**
     * Invoked after a snapshot has been expired (e.g. by Iceberg's
     * {@code ExpireSnapshots} procedure). Hooks may use this to clean up
     * orphan data files or update external metadata catalogs.
     *
     * @param ref        the table whose snapshot expired
     * @param snapshotId the expired snapshot identifier
     */
    default void onSnapshotExpired(ObjectRef ref, long snapshotId) {}

    /**
     * Invoked after a child object's dependency set has been created or
     * replaced (e.g. on CREATE VIEW or CREATE OR REPLACE VIEW).
     *
     * @param child      the dependent object whose edges changed
     * @param newParents the new set of parent objects (may be empty if all
     *                   edges were removed)
     */
    default void onDependenciesChanged(ObjectRef child, Set<ObjectRef> newParents) {}
}
