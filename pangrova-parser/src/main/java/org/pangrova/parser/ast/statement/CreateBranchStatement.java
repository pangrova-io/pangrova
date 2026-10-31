package org.pangrova.parser.ast.statement;

import java.util.Optional;
import java.util.OptionalLong;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents CREATE BRANCH name ON table [FROM TAG tag | FROM SNAPSHOT id].
 */
public record CreateBranchStatement(
        String branchName,
        Identifier tableName,
        Optional<String> fromTag,
        OptionalLong fromSnapshot) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.CREATE_BRANCH;
    }
}
