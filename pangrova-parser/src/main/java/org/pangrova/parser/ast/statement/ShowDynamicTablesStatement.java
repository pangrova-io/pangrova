package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code SHOW DYNAMIC TABLES}.
 */
public record ShowDynamicTablesStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_DYNAMIC_TABLES;
    }
}
