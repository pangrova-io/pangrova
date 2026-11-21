package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code DESCRIBE DYNAMIC TABLE <name>}.
 */
public record DescribeDynamicTableStatement(Identifier name) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.DESCRIBE_DYNAMIC_TABLE;
    }
}
