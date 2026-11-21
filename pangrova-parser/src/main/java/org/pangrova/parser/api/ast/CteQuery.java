package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.CteSearchOrder;
import org.pangrova.parser.api.enums.StatementType;

import java.util.List;
import java.util.Optional;

/**
 * Represents a WITH [RECURSIVE] ... AS (...) CTE query wrapping a set query body.
 * Supports SQL:1999 SEARCH and CYCLE clauses for recursive CTEs.
 */
public record CteQuery(boolean recursive, List<CteDefinition> ctes, Statement body) implements Statement {

    public CteQuery {
        ctes = List.copyOf(ctes);
    }

    /** Backward-compatible constructor without recursive flag. */
    public CteQuery(List<CteDefinition> ctes, Statement body) {
        this(false, ctes, body);
    }

    /**
     * A single CTE definition: name [(columns)] AS (query) [SEARCH ...] [CYCLE ...].
     */
    public record CteDefinition(
            String name,
            List<String> columnList,
            Statement query,
            Optional<SearchClause> searchClause,
            Optional<CycleClause> cycleClause) {

        public CteDefinition {
            columnList = columnList != null ? List.copyOf(columnList) : List.of();
            searchClause = searchClause != null ? searchClause : Optional.empty();
            cycleClause = cycleClause != null ? cycleClause : Optional.empty();
        }

        /** Backward-compatible constructor without search/cycle/columns. */
        public CteDefinition(String name, Statement query) {
            this(name, List.of(), query, Optional.empty(), Optional.empty());
        }
    }

    /**
     * SEARCH DEPTH FIRST | BREADTH FIRST BY columns SET sequenceColumn.
     */
    public record SearchClause(CteSearchOrder order, List<SortItem> orderBy, String sequenceColumn) {
        public SearchClause {
            orderBy = List.copyOf(orderBy);
        }
    }

    /**
     * CYCLE columns SET cycleMarkColumn TO cycleValue DEFAULT noCycleValue USING pathColumn.
     */
    public record CycleClause(
            List<String> cycleColumns,
            String cycleMarkColumn,
            LiteralValue cycleValue,
            LiteralValue noCycleValue,
            String pathColumn) {

        public CycleClause {
            cycleColumns = List.copyOf(cycleColumns);
        }
    }

    @Override
    public StatementType type() {
        return StatementType.CTE_QUERY;
    }
}
