package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents CREATE SCHEMA [IF NOT EXISTS] qualifiedName.
 */
public record CreateSchemaStatement(Identifier schemaName, boolean ifNotExists) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.CREATE_SCHEMA;
    }
}
