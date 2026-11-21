package org.pangrova.parser.api.ast.statement;

import java.util.List;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code SHOW TABLE CONTROLS [FROM|IN catalog[.schema[.table]]]}.
 *
 * <p>{@code scope} is empty for all persisted control records. A catalog or
 * schema scope merges the live catalog listing in as OBSERVED rows.</p>
 */
public record ShowTableControlsStatement(List<String> scope) implements Statement {

    public ShowTableControlsStatement {
        scope = scope == null ? List.of() : List.copyOf(scope);
        if (scope.size() > 3) {
            throw new IllegalArgumentException("Table control scope has at most three parts");
        }
    }

    @Override
    public StatementType type() {
        return StatementType.SHOW_TABLE_CONTROLS;
    }
}
