package org.pangrova.parser.ast;

import java.util.List;
import java.util.Optional;

/**
 * Represents INSERT INTO table [(columns)] VALUES (row1), (row2), ...
 */
public record InsertStatement(
        Identifier tableName,
        Optional<List<String>> columns,
        List<List<Expression>> values) implements Statement {

    public InsertStatement {
        values = values.stream().map(List::copyOf).toList();
    }

    @Override
    public StatementType type() {
        return StatementType.INSERT;
    }
}
