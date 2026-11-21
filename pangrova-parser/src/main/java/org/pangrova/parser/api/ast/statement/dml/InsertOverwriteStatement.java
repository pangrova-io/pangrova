package org.pangrova.parser.api.ast.statement.dml;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents INSERT OVERWRITE table [(columns)] [PARTITION (key=val, ...)] VALUES (...), ...
 */
public record InsertOverwriteStatement(
        Identifier tableName,
        Optional<List<String>> columns,
        Optional<Map<String, Literal>> partitionSpec,
        List<List<Expression>> values) implements Statement {

    public InsertOverwriteStatement {
        values = values.stream().map(List::copyOf).toList();
    }

    /** Backward-compatible constructor without partition spec. */
    public InsertOverwriteStatement(Identifier tableName,
                                     Optional<List<String>> columns,
                                     List<List<Expression>> values) {
        this(tableName, columns, Optional.empty(), values);
    }

    @Override
    public StatementType type() {
        return StatementType.INSERT_OVERWRITE;
    }
}
