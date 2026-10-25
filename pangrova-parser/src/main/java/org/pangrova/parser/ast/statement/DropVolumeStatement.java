package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code DROP VOLUME [IF EXISTS] name}.
 */
public record DropVolumeStatement(Identifier name, boolean ifExists) implements Statement {
}
