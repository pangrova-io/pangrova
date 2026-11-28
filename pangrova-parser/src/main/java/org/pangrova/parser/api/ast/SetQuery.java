package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.SetOperatorType;
import org.pangrova.parser.api.enums.StatementType;

import java.util.List;
import java.util.Optional;

/**
 * Represents a set query combining multiple SELECT queries via UNION ALL,
 * UNION (distinct), INTERSECT, or EXCEPT. The result clauses apply to the combination.
 */
public record SetQuery(List<Query> queries, List<SetOperatorType> operators,
        List<SortItem> orderBy, Optional<Expression> limit, Optional<Expression> offset) implements Statement {

    public SetQuery {
        queries = List.copyOf(queries);
        operators = List.copyOf(operators);
        orderBy = List.copyOf(orderBy);
        if (queries.isEmpty() || operators.size() != queries.size() - 1) {
            throw new IllegalArgumentException("Set query requires one operator between each pair of operands");
        }
    }

    /** Creates an unpaginated set expression. */
    public SetQuery(List<Query> queries, List<SetOperatorType> operators) {
        this(queries, operators, List.of(), Optional.empty(), Optional.empty());
    }

    /** Creates an unpaginated UNION ALL expression. */
    public SetQuery(List<Query> queries) {
        this(queries, buildDefaultOperators(queries.size()));
    }

    private static List<SetOperatorType> buildDefaultOperators(int queryCount) {
        if (queryCount <= 1) return List.of();
        return java.util.Collections.nCopies(queryCount - 1, SetOperatorType.UNION_ALL);
    }

    @Override
    public StatementType type() {
        return StatementType.SET_QUERY;
    }
}
