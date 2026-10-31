package org.pangrova.databuild.api;

/** Data-plane role of one durable artifact work page. */
public enum DataBuildPageKind {
    SOURCE_MAP,
    DEPENDENCY_REDUCE,
    GENERATION_FINALIZE,
    RELATIONAL_COMPUTE,
    RELATIONAL_PARTITION
}
