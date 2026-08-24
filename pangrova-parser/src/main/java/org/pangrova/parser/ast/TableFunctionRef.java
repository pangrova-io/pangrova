package org.pangrova.parser.ast;

import java.util.List;
import java.util.Optional;

/**
 * Represents a table function reference in a TABLE or LATERAL TABLE clause.
 *
 * <p>Models {@code LATERAL TABLE(EXPLODE(arr)) AS t(val)},
 * {@code LATERAL TABLE(POSEXPLODE(arr)) AS t(pos, val)}, and
 * {@code TABLE(GENERATE_SERIES(start, stop))}.</p>
 *
 * @param functionType the table function type
 * @param argument the primary argument (array/map for EXPLODE, start for GENERATE_SERIES)
 * @param endArgument optional second argument (stop for GENERATE_SERIES)
 * @param alias optional table alias
 * @param columnAliases optional output column names
 */
public record TableFunctionRef(
        TableFunctionType functionType,
        Expression argument,
        Optional<Expression> endArgument,
        Optional<String> alias,
        List<String> columnAliases) {

    /**
     * The type of table function.
     */
    public enum TableFunctionType {
        EXPLODE,
        POSEXPLODE,
        GENERATE_SERIES
    }

    public TableFunctionRef(TableFunctionType functionType, Expression argument,
                            Optional<String> alias, List<String> columnAliases) {
        this(functionType, argument, Optional.empty(), alias, columnAliases);
    }

    public TableFunctionRef {
        if (functionType == null) {
            throw new IllegalArgumentException("functionType must not be null");
        }
        if (argument == null) {
            throw new IllegalArgumentException("argument must not be null");
        }
        columnAliases = List.copyOf(columnAliases);
    }
}
