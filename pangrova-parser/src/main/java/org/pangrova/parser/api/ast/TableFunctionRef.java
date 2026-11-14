package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.TableFunctionType;

import java.util.List;
import java.util.Optional;

/**
 * Represents a table function reference in a TABLE or LATERAL TABLE clause.
 *
 * <p>Models {@code LATERAL TABLE(EXPLODE(arr)) AS t(val)},
 * {@code LATERAL TABLE(POSEXPLODE(arr)) AS t(pos, val)}, and
 * {@code TABLE(GENERATE_SERIES(start, stop[, step]))}.</p>
 *
 * @param functionType the table function type
 * @param argument the primary argument (array/map for EXPLODE, start for GENERATE_SERIES)
 * @param endArgument optional second argument (stop for GENERATE_SERIES)
 * @param stepArgument optional third argument (step for GENERATE_SERIES)
 * @param alias optional table alias
 * @param columnAliases optional output column names
 */
public record TableFunctionRef(
        TableFunctionType functionType,
        Expression argument,
        Optional<Expression> endArgument,
        Optional<Expression> stepArgument,
        Optional<String> alias,
        List<String> columnAliases) {

    public TableFunctionRef(TableFunctionType functionType, Expression argument,
                            Optional<String> alias, List<String> columnAliases) {
        this(functionType, argument, Optional.empty(), Optional.empty(), alias, columnAliases);
    }

    public TableFunctionRef(TableFunctionType functionType, Expression argument,
                            Optional<Expression> endArgument,
                            Optional<String> alias, List<String> columnAliases) {
        this(functionType, argument, endArgument, Optional.empty(), alias, columnAliases);
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
