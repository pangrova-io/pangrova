package org.pangrova.parser.ast.statement.maintenance;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents VACUUM table statement.
 */
public record VacuumStatement(Identifier tableName) implements Statement {
}
