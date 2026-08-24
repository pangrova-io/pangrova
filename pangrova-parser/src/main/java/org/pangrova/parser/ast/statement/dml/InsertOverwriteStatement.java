package org.pangrova.parser.ast.statement.dml;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.Statement;

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
}
