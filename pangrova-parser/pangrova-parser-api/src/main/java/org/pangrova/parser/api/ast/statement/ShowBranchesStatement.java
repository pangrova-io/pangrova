package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents SHOW BRANCHES ON table.
 */
public record ShowBranchesStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_BRANCHES;
    }
}
