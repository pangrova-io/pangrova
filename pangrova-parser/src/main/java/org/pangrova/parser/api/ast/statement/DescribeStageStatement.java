package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code DESCRIBE STAGE name}.
 */
public record DescribeStageStatement(Identifier name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_STAGE;
    }
}
