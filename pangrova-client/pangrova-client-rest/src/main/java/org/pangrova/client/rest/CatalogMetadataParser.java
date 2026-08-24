package org.pangrova.client.rest;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses catalog REST API response payloads into client model objects.
 * Package-private helper extracted from {@link PangrovaRestClient}.
 */
final class CatalogMetadataParser {

    private CatalogMetadataParser() {}

    /**
     * Extracts top-level catalog names from a namespaces response.
     */
    static List<String> parseCatalogs(JsonNode root) {
        List<String> catalogs = new ArrayList<>();
        if (root.has("namespaces")) {
            for (var ns : root.get("namespaces")) {
                if (ns.isArray() && !ns.isEmpty()) {
                    catalogs.add(ns.get(0).asText());
                }
            }
        }
        return catalogs;
    }

    /**
     * Extracts table identifiers from a namespace tables response.
     */
    static List<PangrovaRestClient.MetadataTableIdentifier> parseTables(JsonNode root) {
        List<PangrovaRestClient.MetadataTableIdentifier> tables = new ArrayList<>();
        if (root.has("identifiers")) {
            for (var id : root.get("identifiers")) {
                tables.add(toTableIdentifier(id));
            }
        }
        return tables;
    }

    /**
     * Extracts the columns of the current schema from a table metadata response.
     */
    static List<PangrovaRestClient.MetadataColumnInfo> parseColumns(JsonNode root) {
        JsonNode fields = findCurrentSchemaFields(root);
        if (fields == null) {
            return List.of();
        }
        List<PangrovaRestClient.MetadataColumnInfo> columns = new ArrayList<>();
        for (var field : fields) {
            columns.add(toColumnInfo(field));
        }
        return columns;
    }

    private static PangrovaRestClient.MetadataTableIdentifier toTableIdentifier(JsonNode id) {
        String name = id.has("name") ? id.get("name").asText() : "";
        List<String> ns = new ArrayList<>();
        if (id.has("namespace")) {
            for (var part : id.get("namespace")) {
                ns.add(part.asText());
            }
        }
        return new PangrovaRestClient.MetadataTableIdentifier(ns, name);
    }

    private static JsonNode findCurrentSchemaFields(JsonNode root) {
        if (!root.has("metadata") || !root.get("metadata").has("schemas")) {
            return null;
        }
        var metadata = root.get("metadata");
        int currentSchemaId = metadata.has("currentSchemaId")
                ? metadata.get("currentSchemaId").asInt() : 0;
        for (var schema : metadata.get("schemas")) {
            if (schema.has("schemaId") && schema.get("schemaId").asInt() == currentSchemaId
                    && schema.has("fields")) {
                return schema.get("fields");
            }
        }
        return null;
    }

    private static PangrovaRestClient.MetadataColumnInfo toColumnInfo(JsonNode field) {
        return new PangrovaRestClient.MetadataColumnInfo(
                field.has("id") ? field.get("id").asInt() : 0,
                field.has("name") ? field.get("name").asText() : "",
                field.has("type") ? field.get("type").asText() : "string",
                field.has("required") && field.get("required").asBoolean()
        );
    }
}
