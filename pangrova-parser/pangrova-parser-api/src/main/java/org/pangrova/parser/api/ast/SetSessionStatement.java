package org.pangrova.parser.api.ast;

/**
 * Represents SET property = value session configuration statement.
 */
public record SetSessionStatement(String propertyName, String value) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SET_SESSION;
    }
}
