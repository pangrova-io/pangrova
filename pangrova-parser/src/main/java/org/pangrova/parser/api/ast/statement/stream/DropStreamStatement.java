package org.pangrova.parser.api.ast.statement.stream;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code DROP STREAM [IF EXISTS] <name>}.
 *
 * @param name     stream name
 * @param ifExists whether IF EXISTS was specified
 */
public record DropStreamStatement(
        Identifier name,
        boolean ifExists
) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_STREAM;
    }
}
