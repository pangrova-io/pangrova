package org.pangrova.server.coordinator.http.endpoint.catalog;

import java.util.List;
import java.util.Map;

/**
 * Request body for POST /v1/namespaces/{namespace}/tables — creates a new table.
 *
 * @param name       table name
 * @param schema     table schema definition
 * @param properties optional table properties
 * @param location   optional table location
 */
public record CreateTableRequest(
        String name,
        SchemaDefinition schema,
        Map<String, String> properties,
        String location) {

    public CreateTableRequest {
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    /**
     * Schema definition for table creation.
     *
     * @param type   schema type (always "struct")
     * @param fields list of field definitions
     */
    public record SchemaDefinition(String type, List<FieldDefinition> fields) {
        public SchemaDefinition {
            type = type == null ? "struct" : type;
            fields = fields == null ? List.of() : List.copyOf(fields);
        }
    }

    /**
     * Field definition within a schema.
     *
     * @param id       field identifier
     * @param name     field name
     * @param type     field type string
     * @param required whether the field is required (non-nullable)
     */
    public record FieldDefinition(int id, String name, String type, boolean required) {}
}
