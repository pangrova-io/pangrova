package org.pangrova.metadata.api.enums;

/**
 * Policy applied when a parent object with dependents is dropped.
 *
 * <p>Mirrors SQL standard FK-style semantics, but enforced at the catalog
 * level by {@code ObjectDependencyService} rather than by a database engine.
 */
public enum CascadePolicy {

    /**
     * Reject the drop with a {@link DependencyViolationException} that lists
     * all dependents. This is the safe default — matches Trino/Postgres
     * {@code DROP ... RESTRICT}.
     */
    RESTRICT,

    /**
     * Permit the drop, recursively drop all dependents in topological order
     * (children first), and remove the dependency edges. Matches
     * {@code DROP ... CASCADE}. Cascade is bounded by the recorded edges —
     * objects not registered in {@code pangrova_object_dependencies} are not
     * touched.
     */
    CASCADE,

    /**
     * Permit the drop and log a warning per dependent, without dropping dependents.
     * The caller confirms successful deletion through onObjectDropped before
     * the service removes dependency edges; a failed connector drop preserves them.
     * Equivalent to "soft cascade" — dependents become broken (their SQL will
     * fail to resolve next time they are loaded), but the operator gets
     * visibility instead of silent breakage.
     */
    WARN;
}
