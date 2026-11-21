package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents DROP BRANCH name ON table.
 */
public record DropBranchStatement(
        String branchName,
        Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DROP_BRANCH;
    }
}
