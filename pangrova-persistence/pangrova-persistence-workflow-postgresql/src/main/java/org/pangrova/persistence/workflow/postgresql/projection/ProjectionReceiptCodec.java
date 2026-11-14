package org.pangrova.persistence.workflow.postgresql.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.metadata.codec.table.TableMetadataSerializer;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Versioned PostgreSQL receipt encoding; JSON/SDK types never enter the projection contract. */
final class ProjectionReceiptCodec {
    private static final int VERSION = 1;
    private static final TableMetadataSerializer TABLES = new TableMetadataSerializer();

    private ProjectionReceiptCodec() {}

    static String request(MetadataProjectionMutation mutation) {
        ObjectNode root = CanonicalJsonMapper.createObjectNode().put("version", VERSION);
        if (mutation instanceof MetadataProjectionMutation.UpsertTable upsert) {
            root.put("kind", "UPSERT");
            root.set("table", tree(TABLES.serialize(upsert.table())));
        } else if (mutation instanceof MetadataProjectionMutation.DeleteTable deletion) {
            root.put("kind", "DELETE").put("schema", deletion.table().schema()).put("table", deletion.table().table());
        } else if (mutation instanceof MetadataProjectionMutation.ObserveSchemas observation) {
            root.put("kind", "SCHEMAS");
            observation.schemas().forEach(root.putArray("schemas")::add);
        } else {
            throw new IllegalArgumentException("Unsupported projection mutation");
        }
        return root.toString();
    }

    static String result(MetadataProjectionReceipt receipt) {
        ObjectNode root = CanonicalJsonMapper.createObjectNode()
                .put("version", VERSION).put("changed", receipt.changed());
        receipt.previousTable().ifPresent(table -> root.set("previous", tree(TABLES.serialize(table))));
        receipt.allowedTenants().stream().sorted().forEach(root.putArray("tenants")::add);
        return root.toString();
    }

    static MetadataProjectionReceipt decode(UUID id, CatalogDeploymentIdentity scope, String request, String result) {
        try {
            MetadataProjectionMutation mutation = decodeRequest(id, scope, request);
            JsonNode output = versioned(result);
            if (!output.required("changed").isBoolean()) {
                throw new IllegalArgumentException("Projection changed flag is not boolean");
            }
            Optional<TableMetadata> previous = output.has("previous")
                    ? Optional.of(TABLES.deserialize(output.get("previous").toString())) : Optional.empty();
            return new MetadataProjectionReceipt(mutation, output.get("changed").booleanValue(), previous,
                    Set.copyOf(strings(output, "tenants")));
        } catch (RuntimeException failure) {
            throw new MetastoreException("Invalid persisted projection receipt: " + id, failure);
        }
    }

    static MetadataProjectionMutation decodeRequest(UUID id, CatalogDeploymentIdentity scope, String request) {
        try {
            JsonNode input = versioned(request);
            return switch (text(input, "kind")) {
                case "UPSERT" -> new MetadataProjectionMutation.UpsertTable(id, scope,
                        TABLES.deserialize(input.required("table").toString()));
                case "DELETE" -> new MetadataProjectionMutation.DeleteTable(id, scope,
                        new QualifiedTableRef(scope.catalogName(), text(input, "schema"), text(input, "table")));
                case "SCHEMAS" -> new MetadataProjectionMutation.ObserveSchemas(id, scope, strings(input, "schemas"));
                default -> throw new IllegalArgumentException("Unknown projection mutation kind");
            };
        } catch (RuntimeException failure) {
            throw new MetastoreException("Invalid persisted projection request: " + id, failure);
        }
    }

    private static JsonNode versioned(String json) {
        JsonNode root = tree(json);
        if (!root.isObject() || !root.path("version").isIntegralNumber()
                || !root.path("version").canConvertToInt() || root.get("version").intValue() != VERSION) {
            throw new IllegalArgumentException("Unsupported projection receipt format");
        }
        return root;
    }

    private static List<String> strings(JsonNode root, String field) {
        JsonNode values = root.required(field);
        if (!values.isArray()) throw new IllegalArgumentException("Projection field is not an array: " + field);
        List<String> result = new ArrayList<>();
        for (JsonNode value : values) {
            if (!value.isTextual()) throw new IllegalArgumentException("Projection array contains a non-string value");
            result.add(value.textValue());
        }
        return result;
    }

    private static String text(JsonNode root, String field) {
        JsonNode value = root.required(field);
        if (!value.isTextual()) throw new IllegalArgumentException("Projection field is not text: " + field);
        return value.textValue();
    }

    private static JsonNode tree(String json) {
        try {
            return CanonicalJsonMapper.readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new MetastoreException("Cannot decode projection JSON", failure);
        }
    }
}
