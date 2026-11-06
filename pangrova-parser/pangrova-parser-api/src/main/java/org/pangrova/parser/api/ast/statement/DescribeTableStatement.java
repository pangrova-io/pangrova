package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents DESCRIBE table_name.
 */
public record DescribeTableStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_TABLE;
    }
}
