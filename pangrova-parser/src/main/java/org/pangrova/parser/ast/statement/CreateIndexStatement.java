package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

import java.util.List;
import java.util.Map;

/**
 * Represents CREATE INDEX [IF NOT EXISTS] indexName ON tableName (columns) USING indexType [WITH (properties)] [FORCE].
 */
public record CreateIndexStatement(
        String indexName,
        Identifier tableName,
        List<String> columnNames,
        String indexType,
        boolean ifNotExists,
        Map<String, String> properties,
        boolean force) implements Statement {

    public CreateIndexStatement {
        columnNames = List.copyOf(columnNames);
        properties = Map.copyOf(properties);
    }

    /**
     * Backward-compatible constructor without force flag.
     */
    public CreateIndexStatement(
            String indexName,
            Identifier tableName,
            List<String> columnNames,
            String indexType,
            boolean ifNotExists,
            Map<String, String> properties) {
        this(indexName, tableName, columnNames, indexType, ifNotExists, properties, false);
    }
}
