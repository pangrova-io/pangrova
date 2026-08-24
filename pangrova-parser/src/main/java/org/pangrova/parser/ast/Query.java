package org.pangrova.parser.ast;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.ast.expression.RerankClause;

/**
 * Represents a SELECT query with optional DISTINCT modifier.
 */
public record Query(
        boolean distinct,
        List<SelectItem> selectItems,
        List<TableRef> from,
        List<JoinClause> joins,
        Optional<Expression> where,
        List<GroupByItem> groupBy,
        Optional<Expression> having,
        Optional<Expression> qualify,
        List<NamedWindow> windowDefinitions,
        List<SortItem> orderBy,
        Optional<RerankClause> rerankBy,
        Optional<Expression> limit,
        Optional<Expression> offset) implements Statement {

    public Query {
        selectItems = List.copyOf(selectItems);
        from = List.copyOf(from);
        joins = List.copyOf(joins);
        groupBy = List.copyOf(groupBy);
        orderBy = List.copyOf(orderBy);
        windowDefinitions = List.copyOf(windowDefinitions);
    }

    /** Convenience constructor without joins (backward compatible). */
    public Query(
            boolean distinct,
            List<SelectItem> selectItems,
            List<TableRef> from,
            Optional<Expression> where,
            List<GroupByItem> groupBy,
            Optional<Expression> having,
            List<SortItem> orderBy,
            Optional<Expression> limit) {
        this(distinct, selectItems, from, List.of(), where, groupBy, having,
                Optional.empty(), List.of(), orderBy, Optional.empty(), limit, Optional.empty());
    }

    /** Convenience constructor without offset (backward compatible). */
    public Query(
            boolean distinct,
            List<SelectItem> selectItems,
            List<TableRef> from,
            List<JoinClause> joins,
            Optional<Expression> where,
            List<GroupByItem> groupBy,
            Optional<Expression> having,
            List<SortItem> orderBy,
            Optional<Expression> limit) {
        this(distinct, selectItems, from, joins, where, groupBy, having,
                Optional.empty(), List.of(), orderBy, Optional.empty(), limit, Optional.empty());
    }

    /** Convenience constructor without window definitions (backward compatible). */
    public Query(
            boolean distinct,
            List<SelectItem> selectItems,
            List<TableRef> from,
            List<JoinClause> joins,
            Optional<Expression> where,
            List<GroupByItem> groupBy,
            Optional<Expression> having,
            Optional<Expression> qualify,
            List<SortItem> orderBy,
            Optional<RerankClause> rerankBy,
            Optional<Expression> limit,
            Optional<Expression> offset) {
        this(distinct, selectItems, from, joins, where, groupBy, having,
                qualify, List.of(), orderBy, rerankBy, limit, offset);
    }
}
