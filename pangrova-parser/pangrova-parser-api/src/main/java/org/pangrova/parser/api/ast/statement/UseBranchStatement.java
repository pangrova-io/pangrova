package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents USE BRANCH name ON table or USE BRANCH MAIN ON table.
 *
 * <p>When {@code branchName} is "main" (case-insensitive), the session reverts
 * to reading from the main branch of the table.</p>
 */
public record UseBranchStatement(
        String branchName,
        Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.USE_BRANCH;
    }
}
