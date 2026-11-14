package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Comparison operations for L0 pruning. */
@Experimental
public enum ColumnComparisonOperator {
    EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL
}
