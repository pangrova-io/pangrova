package org.pangrova.parser.ast.statement;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;
import org.pangrova.parser.ast.type.DataTypeSpec;

/**
 * Represents CREATE TABLE [IF NOT EXISTS] name (columns) [PRIMARY KEY (...) NOT ENFORCED]
 * [PARTITIONED BY (...)] [SORTED BY (...) [USING ZORDER]] [COMMENT '...'] [WITH/TBLPROPERTIES (...)].
 */
public record CreateTableStatement(
        Identifier tableName,
        List<ColumnDefinition> columns,
        boolean ifNotExists,
        List<String> primaryKeyColumns,
        List<String> partitionColumns,
        List<AlterTableStatement.SortColumn> sortColumns,
        boolean useZOrder,
        Optional<String> comment,
        Map<String, String> properties) implements Statement {

    public CreateTableStatement {
        columns = List.copyOf(columns);
        primaryKeyColumns = List.copyOf(primaryKeyColumns);
        partitionColumns = List.copyOf(partitionColumns);
        sortColumns = List.copyOf(sortColumns);
        properties = Map.copyOf(properties);
    }

    /** Backward-compatible constructor without sort order. */
    public CreateTableStatement(Identifier tableName, List<ColumnDefinition> columns,
                                boolean ifNotExists, List<String> primaryKeyColumns,
                                List<String> partitionColumns, Optional<String> comment,
                                Map<String, String> properties) {
        this(tableName, columns, ifNotExists, primaryKeyColumns, partitionColumns,
                List.of(), false, comment, properties);
    }

    /**
     * Column name + structured type with optional auto-embedding specification.
     *
     * @param name          the column name
     * @param type          the structured column type
     * @param nullable      whether column allows NULLs (true = nullable, false = NOT NULL)
     * @param defaultValue  optional default value expression as string
     * @param embeddingSpec optional auto-embedding specification for GENERATED ALWAYS AS columns
     */
    public record ColumnDefinition(String name, DataTypeSpec type, boolean nullable,
                                   Optional<String> defaultValue,
                                   Optional<AutoEmbeddingColumnSpec> embeddingSpec) {

        /** Backward-compatible constructor for columns without auto-embedding. */
        public ColumnDefinition(String name, DataTypeSpec type) {
            this(name, type, true, Optional.empty(), Optional.empty());
        }

        /** Constructor with embedding spec only (existing API). */
        public ColumnDefinition(String name, DataTypeSpec type, Optional<AutoEmbeddingColumnSpec> embeddingSpec) {
            this(name, type, true, Optional.empty(), embeddingSpec);
        }

        /**
         * Returns the canonical string representation of the column type,
         * suitable for display, logging, and serialization.
         */
        public String typeAsString() {
            return type.toCanonicalString();
        }
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_TABLE;
    }
}
