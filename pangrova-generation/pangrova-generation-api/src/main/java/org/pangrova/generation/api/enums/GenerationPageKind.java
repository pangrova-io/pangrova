package org.pangrova.generation.api.enums;

/** Data-plane role of one durable artifact work page. */
public enum GenerationPageKind {
    SOURCE_MAP,
    DEPENDENCY_REDUCE,
    GENERATION_FINALIZE,
    RELATIONAL_COMPUTE,
    RELATIONAL_PARTITION;

    /** Relational pages obtain dependencies from the stage's input exchanges. */
    public boolean isRelational() {
        return this == RELATIONAL_COMPUTE || this == RELATIONAL_PARTITION;
    }
}
