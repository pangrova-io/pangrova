package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Function kind discriminator. */
@Experimental
public enum ConnectorFunctionKind {
    /** f(scalar...) -> scalar */
    SCALAR,
    /** f(scalar...) -> scalar folded over a multiset (SUM, AVG, ...). */
    AGGREGATE,
    /** f(...) -> relation (returns a row stream). */
    TABLE
}
