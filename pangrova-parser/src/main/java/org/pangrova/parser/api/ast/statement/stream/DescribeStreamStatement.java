package org.pangrova.parser.api.ast.statement.stream;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code DESCRIBE STREAM <name>}.
 */
public record DescribeStreamStatement(Identifier name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_STREAM;
    }
}
