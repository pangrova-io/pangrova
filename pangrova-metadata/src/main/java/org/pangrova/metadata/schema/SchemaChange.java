package org.pangrova.metadata.schema;

import org.pangrova.metadata.api.object.ColumnType;

/**
 * Sealed interface representing possible schema evolution operations.
 */
public sealed interface SchemaChange permits
        SchemaChange.AddColumn,
        SchemaChange.DropColumn,
        SchemaChange.RenameColumn,
        SchemaChange.ChangeType,
        SchemaChange.ChangeNullability {

    record AddColumn(String name, ColumnType type, boolean nullable) implements SchemaChange {}

    record DropColumn(String name) implements SchemaChange {}

    record RenameColumn(String oldName, String newName) implements SchemaChange {}

    record ChangeType(String name, ColumnType newType) implements SchemaChange {}

    record ChangeNullability(String name, boolean nullable) implements SchemaChange {}
}
