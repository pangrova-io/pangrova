package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents SHOW CLUSTERING STATUS FOR TABLE t.
 *
 * @param tableName qualified table name
 */
public record ShowClusteringStatusStatement(Identifier tableName) implements Statement {
}
