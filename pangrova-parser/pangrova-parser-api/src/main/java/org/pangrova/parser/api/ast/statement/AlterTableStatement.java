package org.pangrova.parser.api.ast.statement;

import java.util.List;
import java.util.Optional;

import java.util.OptionalLong;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;
import org.pangrova.parser.api.ast.type.DataTypeSpec;

/**
 * Represents ALTER TABLE operations: ADD COLUMN, DROP COLUMN, RENAME COLUMN, CHANGE TYPE,
 * SET PARTITION SPEC.
 */
public sealed interface AlterTableStatement extends Statement {

    Identifier tableName();

    record AddColumn(Identifier tableName, String columnName, DataTypeSpec columnType,
                     Optional<Literal> defaultValue,
                     Optional<AutoEmbeddingColumnSpec> embeddingSpec) implements AlterTableStatement {

        /** Backward-compatible constructor without embedding spec. */
        public AddColumn(Identifier tableName, String columnName, DataTypeSpec columnType,
                         Optional<Literal> defaultValue) {
            this(tableName, columnName, columnType, defaultValue, Optional.empty());
        }

        /** Backward-compatible constructor without default value or embedding spec. */
        public AddColumn(Identifier tableName, String columnName, DataTypeSpec columnType) {
            this(tableName, columnName, columnType, Optional.empty(), Optional.empty());
        }

        /** Returns the canonical string representation of the column type. */
        public String columnTypeAsString() {
            return columnType.toCanonicalString();
        }
    }

    record DropColumn(Identifier tableName, String columnName) implements AlterTableStatement {}

    record RenameTable(Identifier tableName, Identifier newName) implements AlterTableStatement {}

    record RenameColumn(Identifier tableName, String oldName, String newName) implements AlterTableStatement {}

    record ChangeColumnType(
            Identifier tableName,
            String columnName,
            DataTypeSpec newType) implements AlterTableStatement {

        /** Returns the canonical string representation of the new column type. */
        public String newTypeAsString() {
            return newType.toCanonicalString();
        }
    }

    /**
     * Represents ALTER TABLE ... SET PARTITION SPEC (...).
     * Each transform is either identity (column name) or function-based (e.g. month(ts)).
     */
    record SetPartitionSpec(Identifier tableName,
                            List<PartitionTransform> transforms) implements AlterTableStatement {
        public SetPartitionSpec {
            transforms = List.copyOf(transforms);
        }
    }

    /** Represents ALTER TABLE ... SET SORT ORDER (col1 ASC, col2 DESC) [USING ZORDER]. */
    record SetSortOrder(Identifier tableName,
                        List<SortColumn> sortColumns,
                        boolean useZOrder) implements AlterTableStatement {

        public SetSortOrder {
            sortColumns = List.copyOf(sortColumns);
        }

        /** Backward-compatible constructor without zOrder flag. */
        public SetSortOrder(Identifier tableName, List<SortColumn> sortColumns) {
            this(tableName, sortColumns, false);
        }
    }

    /** Represents ALTER TABLE ... CREATE BRANCH branchName [RETAIN n DAYS]. */
    record CreateBranch(Identifier tableName, String branchName,
                        OptionalLong retentionDays) implements AlterTableStatement {

        /** Backward-compatible constructor without retention. */
        public CreateBranch(Identifier tableName, String branchName) {
            this(tableName, branchName, OptionalLong.empty());
        }
    }

    /** Represents ALTER TABLE ... DROP BRANCH [IF EXISTS] branchName. */
    record DropBranch(Identifier tableName, String branchName,
                      boolean ifExists) implements AlterTableStatement {

        /** Backward-compatible constructor without ifExists. */
        public DropBranch(Identifier tableName, String branchName) {
            this(tableName, branchName, false);
        }
    }

    /** Represents ALTER TABLE ... CREATE TAG tagName. */
    record CreateTag(Identifier tableName, String tagName) implements AlterTableStatement {}

    /** Represents ALTER TABLE ... DROP TAG [IF EXISTS] tagName. */
    record DropTag(Identifier tableName, String tagName,
                   boolean ifExists) implements AlterTableStatement {

        /** Backward-compatible constructor without ifExists. */
        public DropTag(Identifier tableName, String tagName) {
            this(tableName, tagName, false);
        }
    }

    /** Represents ALTER TABLE ... ADOPT, adopting an externally written table as LINKED. */
    record Adopt(Identifier tableName, java.util.Map<String, String> properties)
            implements AlterTableStatement {

        public Adopt {
            properties = properties == null ? java.util.Map.of() : java.util.Map.copyOf(properties);
        }

        /** Adopt without policy properties. */
        public Adopt(Identifier tableName) {
            this(tableName, java.util.Map.of());
        }
    }

    /** Represents ALTER TABLE ... UNLINK, removing control state without dropping the table. */
    record Unlink(Identifier tableName) implements AlterTableStatement {}

    /** Represents ALTER TABLE ... ALTER COLUMN col SET TAG key = 'value'. */
    record SetColumnTag(Identifier tableName, String columnName,
                        String tagKey, String tagValue) implements AlterTableStatement {}

    /** Represents ALTER TABLE ... ALTER COLUMN col UNSET TAG key. */
    record UnsetColumnTag(Identifier tableName, String columnName,
                          String tagKey) implements AlterTableStatement {}

    /**
     * A sort column with optional direction.
     *
     * @param columnName the column name
     * @param ascending  true for ASC (default), false for DESC
     */
    record SortColumn(String columnName, boolean ascending) {

        /** Creates an ascending sort column. */
        public static SortColumn asc(String columnName) {
            return new SortColumn(columnName, true);
        }

        /** Creates a descending sort column. */
        public static SortColumn desc(String columnName) {
            return new SortColumn(columnName, false);
        }
    }

    /**
     * A partition transform: identity (just a column) or function-based.
     *
     * <p>Single-arg transforms like {@code month(ts)} store the column in {@code arguments}.
     * Multi-arg transforms like {@code bucket(col1, col2, 16)} store all arguments
     * (column names and literal values) in {@code arguments}.</p>
     *
     * @param functionName the transform function name, or null for identity
     * @param arguments    the transform arguments (column names and/or literal values)
     */
    record PartitionTransform(String functionName, List<String> arguments) {

        public PartitionTransform {
            arguments = List.copyOf(arguments);
        }

        /** Creates an identity transform. */
        public static PartitionTransform identity(String columnName) {
            return new PartitionTransform(null, List.of(columnName));
        }

        /** Creates a single-arg function transform (e.g. month(ts), year(date)). */
        public static PartitionTransform function(String functionName, String columnName) {
            return new PartitionTransform(functionName, List.of(columnName));
        }

        /** Creates a multi-arg function transform (e.g. bucket(col1, col2, 16)). */
        public static PartitionTransform function(String functionName, List<String> arguments) {
            return new PartitionTransform(functionName, arguments);
        }

        /** Returns the first argument (primary column name), or empty string if no arguments. */
        public String columnName() {
            return arguments.isEmpty() ? "" : arguments.get(0);
        }

        public boolean isIdentity() {
            return functionName == null;
        }
    }

    @Override
    default StatementType type() {
        return StatementType.ALTER_TABLE;
    }
}
