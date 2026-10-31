package org.pangrova.parser.ast;

/**
 * Represents SHOW SCHEMAS FROM/IN catalog.
 */
public record ShowSchemasStatement(String catalogName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_SCHEMAS;
    }
}
