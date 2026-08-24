package org.pangrova.metadata.dependency;

/**
 * Policy applied when a parent object with dependents is dropped.
 *
 * <p>Mirrors SQL standard FK-style semantics, but enforced at the catalog
 * level by {@link ObjectDependencyService} rather than by a database engine.
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
     * Permit the drop, log a warning per dependent, and remove the dependency
     * edges (so the catalog state is consistent), but do NOT drop dependents.
     * Equivalent to "soft cascade" — dependents become broken (their SQL will
     * fail to resolve next time they are loaded), but the operator gets
     * visibility instead of silent breakage.
     */
    WARN;
}
