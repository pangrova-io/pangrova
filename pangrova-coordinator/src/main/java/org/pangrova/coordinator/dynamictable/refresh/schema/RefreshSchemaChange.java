package org.pangrova.coordinator.dynamictable.refresh.schema;

import java.util.Objects;

/**
 * Represents a single schema change detected between two schema versions.
 */
public sealed interface RefreshSchemaChange
        permits RefreshSchemaChange.ColumnAdded,
                RefreshSchemaChange.ColumnRemoved,
                RefreshSchemaChange.ColumnTypeChanged {

    /** Name of the affected column. */
    String columnName();

    /** A new column was added to the source schema. */
    record ColumnAdded(String columnName, String columnType) implements RefreshSchemaChange {
        public ColumnAdded {
            Objects.requireNonNull(columnName, "columnName must not be null");
            Objects.requireNonNull(columnType, "columnType must not be null");
        }
    }

    /** A column was removed from the source schema. */
    record ColumnRemoved(String columnName) implements RefreshSchemaChange {
        public ColumnRemoved {
            Objects.requireNonNull(columnName, "columnName must not be null");
        }
    }

    /** A column's type was changed in the source schema. */
    record ColumnTypeChanged(String columnName, String oldType, String newType) implements RefreshSchemaChange {
        public ColumnTypeChanged {
            Objects.requireNonNull(columnName, "columnName must not be null");
            Objects.requireNonNull(oldType, "oldType must not be null");
            Objects.requireNonNull(newType, "newType must not be null");
        }
    }
}
