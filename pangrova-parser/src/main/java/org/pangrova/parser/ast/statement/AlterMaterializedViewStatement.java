package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code ALTER MATERIALIZED VIEW <name> <action>}.
 */
public record AlterMaterializedViewStatement(
        Identifier name,
        AlterMaterializedViewAction action
) implements Statement {
}
