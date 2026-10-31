package org.pangrova.parser.ast.statement.stream;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code DESCRIBE STREAM <name>}.
 */
public record DescribeStreamStatement(Identifier name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_STREAM;
    }
}
