package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents DESCRIBE table_name.
 */
public record DescribeTableStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_TABLE;
    }
}
