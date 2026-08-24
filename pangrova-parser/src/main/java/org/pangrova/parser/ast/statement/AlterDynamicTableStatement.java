package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code ALTER DYNAMIC TABLE <name> <action>}.
 */
public record AlterDynamicTableStatement(
        Identifier name,
        AlterDynamicTableAction action
) implements Statement {
}
