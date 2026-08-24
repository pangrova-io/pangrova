package org.pangrova.parser.ast;

/**
 * Represents a single element in a GROUP BY clause.
 *
 * <p>A GROUP BY clause can contain plain expressions, GROUPING SETS,
 * CUBE, or ROLLUP elements. This sealed interface models all four
 * variants, enabling exhaustive pattern matching in visitors and
 * printers.</p>
 *
 * <p>When the GROUP BY clause contains only plain expressions
 * (the common case), each expression is wrapped in a
 * {@link GroupByExpression}. This preserves backward compatibility
 * with code that iterates over group-by items.</p>
 */
public sealed interface GroupByItem
        permits GroupByExpression, GroupingSets, CubeGroupBy, RollupGroupBy {
}
