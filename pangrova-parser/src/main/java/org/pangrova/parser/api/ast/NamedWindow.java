package org.pangrova.parser.api.ast;

import java.util.List;
import java.util.Optional;

/**
 * Represents a named window definition in the WINDOW clause.
 * Example: WINDOW w AS (PARTITION BY x ORDER BY y ROWS BETWEEN ...)
 */
public record NamedWindow(
        String name,
        List<Expression> partitionBy,
        List<SortItem> orderBy,
        Optional<WindowSpec.WindowFrame> frame) {

    public NamedWindow {
        partitionBy = List.copyOf(partitionBy);
        orderBy = List.copyOf(orderBy);
    }
}
