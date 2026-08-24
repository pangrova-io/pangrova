package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents SHOW BRANCHES ON table.
 */
public record ShowBranchesStatement(Identifier tableName) implements Statement {
}
