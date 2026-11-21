package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents SHOW SCHEMAS FROM/IN catalog.
 */
public record ShowSchemasStatement(String catalogName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_SCHEMAS;
    }
}
