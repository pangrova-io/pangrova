package org.pangrova.materialization.api.enums;

/** GC decision recorded for a generation. */
public enum GcDecision {
    RETAIN,
    COLLECT,
    DEFER
}
