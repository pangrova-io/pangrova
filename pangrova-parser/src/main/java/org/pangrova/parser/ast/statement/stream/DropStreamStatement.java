package org.pangrova.parser.ast.statement.stream;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

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
}
