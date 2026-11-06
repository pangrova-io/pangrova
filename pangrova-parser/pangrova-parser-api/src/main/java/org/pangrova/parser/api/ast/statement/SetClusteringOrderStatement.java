package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

import java.util.List;

/**
 * Represents ALTER TABLE t SET CLUSTERING ORDER (col1, col2) [MODE INTERLEAVE|LINEAR].
 *
 * @param tableName   qualified table name
 * @param columnNames clustering columns in order
 * @param mode        clustering mode (INTERLEAVE or LINEAR); null means default INTERLEAVE
 */
public record SetClusteringOrderStatement(
        Identifier tableName,
        List<String> columnNames,
        String mode) implements Statement {

    public SetClusteringOrderStatement {
        columnNames = List.copyOf(columnNames);
    }

    @Override
    public StatementType type() {
        return StatementType.SET_CLUSTERING_ORDER;
    }
}
