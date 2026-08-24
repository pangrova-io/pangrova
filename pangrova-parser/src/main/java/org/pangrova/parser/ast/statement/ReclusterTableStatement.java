package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

import java.util.Optional;

/**
 * Represents RECLUSTER TABLE t [WHERE partition_col = 'value'].
 *
 * @param tableName       qualified table name
 * @param whereCondition  optional partition filter expression
 */
public record ReclusterTableStatement(
        Identifier tableName,
        Optional<Expression> whereCondition) implements Statement {
}
