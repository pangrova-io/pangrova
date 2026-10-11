package org.pangrova.parser.ast;

import java.util.List;

/** A VALUES table constructor with an optional declared column-name list. */
public record ValuesTable(List<List<Expression>> rows, List<String> columnAliases) {
    public ValuesTable {
        rows = rows.stream().map(List::copyOf).toList();
        columnAliases = List.copyOf(columnAliases);
        if (rows.isEmpty() || rows.get(0).isEmpty()) {
            throw new IllegalArgumentException("VALUES requires a nonempty row");
        }
        int width = rows.get(0).size();
        if (rows.stream().anyMatch(row -> row.size() != width)) {
            throw new IllegalArgumentException("VALUES rows must have equal width");
        }
        if (!columnAliases.isEmpty() && columnAliases.size() != width) {
            throw new IllegalArgumentException("VALUES column alias count must match row width");
        }
    }
}
