package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

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

    @Override
    public StatementType type() {
        return StatementType.RECLUSTER_TABLE;
    }
}
