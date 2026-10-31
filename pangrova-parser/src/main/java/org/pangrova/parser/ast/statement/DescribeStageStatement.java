package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code DESCRIBE STAGE name}.
 */
public record DescribeStageStatement(Identifier name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_STAGE;
    }
}
