package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.SqlJoinType;

import java.util.List;
import java.util.Optional;

/**
 * Represents a JOIN clause between two table references.
 */
public record JoinClause(
        SqlJoinType joinType,
        TableRef right,
        Optional<Expression> condition,
        List<String> usingColumns) {

    public JoinClause {
        usingColumns = List.copyOf(usingColumns);
    }

    /** Creates a JOIN with ON condition. */
    public JoinClause(SqlJoinType joinType, TableRef right, Optional<Expression> condition) {
        this(joinType, right, condition, List.of());
    }

    /** Creates a JOIN with USING columns. */
    public static JoinClause withUsing(SqlJoinType joinType, TableRef right, List<String> usingColumns) {
        return new JoinClause(joinType, right, Optional.empty(), List.copyOf(usingColumns));
    }

}
