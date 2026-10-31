package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code DESCRIBE DYNAMIC TABLE <name>}.
 */
public record DescribeDynamicTableStatement(Identifier name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_DYNAMIC_TABLE;
    }
}
