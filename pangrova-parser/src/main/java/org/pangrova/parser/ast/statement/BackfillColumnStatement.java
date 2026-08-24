package org.pangrova.parser.ast.statement;

import java.util.Optional;
import java.util.OptionalInt;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents ALTER TABLE ... BACKFILL COLUMN ... [WHERE ...] [BATCH_SIZE n] [CONCURRENCY c].
 *
 * <p>Triggers distributed embedding backfill for the specified column on matching rows.
 */
public record BackfillColumnStatement(
        Identifier tableName,
        String columnName,
        Optional<Expression> whereClause,
        OptionalInt batchSize,
        OptionalInt concurrency) implements Statement {
}
