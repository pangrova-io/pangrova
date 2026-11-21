package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code SHOW DYNAMIC TABLES}.
 */
public record ShowDynamicTablesStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_DYNAMIC_TABLES;
    }
}
