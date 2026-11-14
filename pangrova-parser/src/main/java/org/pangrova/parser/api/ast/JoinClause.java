package org.pangrova.parser.api.ast;

import java.util.List;
import java.util.Optional;

/**
 * Represents a JOIN clause between two table references.
 */
public record JoinClause(
        JoinType joinType,
        TableRef right,
        Optional<Expression> condition,
        List<String> usingColumns) {

    public JoinClause {
        usingColumns = List.copyOf(usingColumns);
    }

    /** Creates a JOIN with ON condition. */
    public JoinClause(JoinType joinType, TableRef right, Optional<Expression> condition) {
        this(joinType, right, condition, List.of());
    }

    /** Creates a JOIN with USING columns. */
    public static JoinClause withUsing(JoinType joinType, TableRef right, List<String> usingColumns) {
        return new JoinClause(joinType, right, Optional.empty(), List.copyOf(usingColumns));
    }

    /** Supported join types. */
    public enum JoinType {
        INNER, LEFT, RIGHT, FULL, CROSS, COMMA
    }
}
