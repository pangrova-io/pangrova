package org.pangrova.generation.api.execution;

/** Data-plane role of one durable artifact work page. */
public enum GenerationPageKind {
    SOURCE_MAP,
    DEPENDENCY_REDUCE,
    GENERATION_FINALIZE,
    RELATIONAL_COMPUTE,
    RELATIONAL_PARTITION
}
