package org.pangrova.indexing.api;

/** Data-plane role of one durable artifact work page. */
public enum ArtifactBuildPageKind {
    SOURCE_MAP,
    DEPENDENCY_REDUCE,
    GENERATION_FINALIZE,
    RELATIONAL_COMPUTE,
    RELATIONAL_PARTITION
}
