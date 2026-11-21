package org.pangrova.metadata.api.enums;

/** Kind of catalog function. */
public enum FunctionKind {
    /** {@code f(scalar...) → scalar} */
    SCALAR,
    /** {@code f(scalar...) → scalar} folded over a multiset (SUM, AVG, ...). */
    AGGREGATE,
    /** {@code f(...) → relation} (returns a row stream). */
    TABLE
}
