package org.pangrova.materialization.domain;

/**
 * Relationship represented by one dependency edge.
 */
public enum DependencyKind {

    /** The downstream definition directly references the upstream object. */
    DIRECT,

    /** The downstream object inherits the dependency through an intermediate object. */
    INDIRECT
}
