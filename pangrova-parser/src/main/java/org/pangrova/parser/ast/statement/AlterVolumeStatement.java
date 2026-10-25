package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code ALTER VOLUME <name> <action>}.
 */
public record AlterVolumeStatement(
        Identifier name,
        AlterVolumeAction action) implements Statement {
}
