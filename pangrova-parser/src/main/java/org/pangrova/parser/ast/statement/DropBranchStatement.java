package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents DROP BRANCH name ON table.
 */
public record DropBranchStatement(
        String branchName,
        Identifier tableName) implements Statement {
}
