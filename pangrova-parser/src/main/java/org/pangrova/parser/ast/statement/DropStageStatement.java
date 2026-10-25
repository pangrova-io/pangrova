package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code DROP STAGE [IF EXISTS] name}.
 */
public record DropStageStatement(Identifier name, boolean ifExists) implements Statement {
}
