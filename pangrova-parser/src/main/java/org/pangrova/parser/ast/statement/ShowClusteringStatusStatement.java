package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents SHOW CLUSTERING STATUS FOR TABLE t.
 *
 * @param tableName qualified table name
 */
public record ShowClusteringStatusStatement(Identifier tableName) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_CLUSTERING_STATUS;
    }
}
