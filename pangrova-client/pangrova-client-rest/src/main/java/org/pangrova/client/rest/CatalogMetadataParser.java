package org.pangrova.client.rest;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/** Validates the sole catalog REST metadata contract; malformed responses are never empty metadata. */
final class CatalogMetadataParser {
    private CatalogMetadataParser() {}

    static List<String> parseCatalogs(JsonNode root) {
        return namespaces(root).stream().map(parts -> parts.get(0)).distinct().sorted().toList();
    }

    static List<String> parseSchemas(JsonNode root, String catalog) {
        var schemas = new ArrayList<String>();
        for (var parts : namespaces(root)) {
            require(parts.size() == 2 && parts.get(0).equals(catalog), "Schema namespace does not match catalog");
            schemas.add(parts.get(1));
        }
        return schemas.stream().distinct().sorted().toList();
    }

    private static List<List<String>> namespaces(JsonNode root) {
        var result = new ArrayList<List<String>>();
        for (var namespace : array(root, "namespaces")) result.add(namespaceParts(namespace));
        return List.copyOf(result);
    }

    static List<PangrovaRestClient.MetadataTableIdentifier> parseTables(JsonNode root) {
        var tables = new ArrayList<PangrovaRestClient.MetadataTableIdentifier>();
        for (var id : array(root, "identifiers")) {
            tables.add(new PangrovaRestClient.MetadataTableIdentifier(namespaceParts(id.get("namespace")), text(id, "name")));
        }
        return List.copyOf(tables);
    }

    static List<PangrovaRestClient.MetadataColumnInfo> parseColumns(JsonNode root) {
        JsonNode metadata = root.get("metadata");
        require(metadata != null && metadata.isObject(), "Missing table metadata");
        int current = integer(metadata, "currentSchemaId");
        JsonNode selected = null;
        for (var schema : array(metadata, "schemas")) {
            if (integer(schema, "schemaId") == current) {
                require(selected == null, "Duplicate current schema");
                selected = schema;
            }
        }
        require(selected != null, "Current schema is absent");
        var columns = new ArrayList<PangrovaRestClient.MetadataColumnInfo>();
        for (var field : array(selected, "fields")) {
            JsonNode required = field.get("required");
            require(required != null && required.isBoolean(), "Column required must be boolean");
            columns.add(new PangrovaRestClient.MetadataColumnInfo(integer(field, "id"), text(field, "name"),
                    text(field, "type"), required.booleanValue()));
        }
        return List.copyOf(columns);
    }

    private static List<String> namespaceParts(JsonNode node) {
        require(node != null && node.isArray() && !node.isEmpty(), "Namespace must contain components");
        var parts = new ArrayList<String>();
        for (var part : node) {
            require(part.isTextual() && !part.textValue().isEmpty(), "Namespace component must be nonempty text");
            require(!part.textValue().contains("\u001f"), "Namespace component contains reserved separator");
            parts.add(part.textValue());
        }
        return List.copyOf(parts);
    }

    private static JsonNode array(JsonNode node, String field) {
        require(node != null && node.isObject(), "Expected metadata object");
        var value = node.get(field);
        require(value != null && value.isArray(), "Expected metadata array: " + field);
        return value;
    }

    private static String text(JsonNode node, String field) {
        var value = node.get(field);
        require(value != null && value.isTextual() && !value.textValue().isEmpty(), "Expected metadata text: " + field);
        return value.textValue();
    }

    private static int integer(JsonNode node, String field) {
        var value = node.get(field);
        require(value != null && value.isIntegralNumber() && value.canConvertToInt(), "Expected metadata integer: " + field);
        return value.intValue();
    }

    private static void require(boolean valid, String message) {
        if (!valid) throw new PangrovaProtocolException("CATALOG_METADATA_INVALID", message, 200);
    }
}
