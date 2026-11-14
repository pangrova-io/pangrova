package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.StatementType;

import java.util.List;

/**
 * Represents a set query combining multiple SELECT queries via UNION ALL,
 * UNION (distinct), INTERSECT, or EXCEPT.
 */
public record SetQuery(List<Query> queries, List<SetOperatorType> operators) implements Statement {

    public SetQuery {
        queries = List.copyOf(queries);
        operators = List.copyOf(operators);
    }

    /** Backward-compatible constructor for UNION ALL only. */
    public SetQuery(List<Query> queries) {
        this(queries, buildDefaultOperators(queries.size()));
    }

    private static List<SetOperatorType> buildDefaultOperators(int queryCount) {
        if (queryCount <= 1) return List.of();
        return java.util.Collections.nCopies(queryCount - 1, SetOperatorType.UNION_ALL);
    }

    public enum SetOperatorType {
        UNION_ALL,
        UNION_DISTINCT,
        INTERSECT,
        EXCEPT
    }

    @Override
    public StatementType type() {
        return StatementType.SET_QUERY;
    }
}
