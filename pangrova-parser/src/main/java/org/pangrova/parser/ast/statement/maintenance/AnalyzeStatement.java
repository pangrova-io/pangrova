package org.pangrova.parser.ast.statement.maintenance;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code ANALYZE TABLE [COMPUTE STATISTICS]}: collects connector statistics
 * and persists them against the controlled table's current version token.
 */
public record AnalyzeStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.ANALYZE;
    }
}
