package org.pangrova.parser.ast;

import java.util.List;

/**
 * Represents an explicit {@code GROUPING SETS} clause in GROUP BY.
 *
 * <p>Each element in {@code sets} is a list of expressions forming
 * one grouping set. An empty inner list represents the grand total
 * grouping set {@code ()}.</p>
 *
 * <p>Example: {@code GROUP BY GROUPING SETS ((a, b), (a), ())}
 * produces three sets: {@code [a,b]}, {@code [a]}, {@code []}.</p>
 *
 * @param sets the list of grouping sets, each a list of expressions
 */
public record GroupingSets(List<List<Expression>> sets) implements GroupByItem {

    public GroupingSets {
        if (sets == null) {
            throw new IllegalArgumentException("sets must not be null");
        }
        sets = sets.stream()
                .map(List::copyOf)
                .toList();
    }
}
