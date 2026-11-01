package org.pangrova.spi.connector.metadata.table;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.connector.data.type.PangrovaType;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable metadata describing a connector table, including its name,
 * column definitions, and optional properties.
 *
 * @param table        the schema-qualified table name
 * @param columns      the ordered list of column names
 * @param typedColumns the ordered list of typed column descriptors (empty if unavailable)
 * @param properties   additional table properties
 */
@Experimental
public record ConnectorTableMetadata(
        SchemaTableName table,
        List<String> columns,
        List<Column> typedColumns,
        Map<String, String> properties) {

    /**
     * Creates table metadata with validation.
     *
     * @param table        the schema-qualified table name
     * @param columns      the ordered list of column names
     * @param typedColumns the ordered list of typed column descriptors
     * @param properties   additional table properties
     * @throws NullPointerException if any argument is null
     */
    public ConnectorTableMetadata {
        checkNotNull(table, "table must not be null");
        columns = List.copyOf(checkNotNull(columns, "columns must not be null"));
        typedColumns = List.copyOf(checkNotNull(typedColumns, "typedColumns must not be null"));
        properties = Map.copyOf(checkNotNull(properties, "properties must not be null"));
    }

    /** Backward-compatible constructor without typed columns. */
    public ConnectorTableMetadata(SchemaTableName table, List<String> columns, Map<String, String> properties) {
        this(table, columns, List.of(), properties);
    }

    /**
     * Typed column descriptor carrying name, PangrovaType, and optional
     * source field ID for schema evolution (e.g. Iceberg field IDs).
     *
     * @param name              column name
     * @param type              Pangrova canonical type
     * @param fieldId           source-specific field identifier (empty if not applicable)
     * @param parameterizedType full parameterized type with element/key/value info (nullable)
     */
    public record Column(String name, PangrovaType type, OptionalInt fieldId,
                         org.pangrova.spi.connector.data.type.Type parameterizedType) {

        public Column {
            checkNotNull(name, "name must not be null");
            checkNotNull(type, "type must not be null");
            checkNotNull(fieldId, "fieldId must not be null");
        }

        /** Convenience constructor without field ID or parameterized type. */
        public Column(String name, PangrovaType type) {
            this(name, type, OptionalInt.empty(), null);
        }

        /** Convenience constructor with field ID but no parameterized type. */
        public Column(String name, PangrovaType type, OptionalInt fieldId) {
            this(name, type, fieldId, null);
        }
    }
}
