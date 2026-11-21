package org.pangrova.governance.api.enums;

/** How an output column depends on its inputs in a planned or published lineage fact. */
public enum ColumnDerivationKind {
    DIRECT,
    TRANSFORM,
    AGGREGATE
}
