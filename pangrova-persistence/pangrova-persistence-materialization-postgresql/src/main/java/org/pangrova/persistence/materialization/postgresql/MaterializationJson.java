package org.pangrova.persistence.materialization.postgresql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.pangrova.materialization.api.domain.ColumnDefinition;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.OutputSchema;
import org.pangrova.materialization.api.domain.PolicySnapshot;
import org.pangrova.materialization.api.domain.SourceVector;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.connector.api.materialization.SourceSnapshotRef;

/** Stable JSON codecs for materialization persisted structures. */
final class MaterializationJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MaterializationJson() {}

    static ObjectMapper mapper() {
        return MAPPER;
    }

    static JsonNode tree(String json) {
        if (json == null) {
            throw corrupt("missing persisted JSON");
        }
        try {
            return MAPPER.readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw MaterializationException.repositoryFailure(
                    "persisted materialization JSON is malformed", failure);
        }
    }

    static String sessionProperties(Map<String, String> properties) {
        ObjectNode node = MAPPER.createObjectNode();
        new TreeMap<>(properties).forEach(node::put);
        return node.toString();
    }

    static String definerGroups(Set<String> groups) {
        ArrayNode array = MAPPER.createArrayNode();
        groups.stream().sorted().forEach(array::add);
        return array.toString();
    }

    static Set<String> definerGroups(JsonNode node) {
        if (node == null || !node.isArray()) {
            throw corrupt("definition definer groups must be a JSON array");
        }
        Set<String> groups = new TreeSet<>();
        for (JsonNode entry : node) {
            if (!entry.isTextual() || entry.asText().isBlank()) {
                throw corrupt("definition definer group must be a non-blank string");
            }
            groups.add(entry.asText());
        }
        return Set.copyOf(groups);
    }

    static Map<String, String> sessionProperties(JsonNode node) {
        if (node == null || !node.isObject()) {
            throw corrupt("definition session properties must be a JSON object");
        }
        Map<String, String> properties = new TreeMap<>();
        node.fields().forEachRemaining(property -> {
            if (!property.getValue().isTextual()) {
                throw corrupt("definition session property " + property.getKey()
                        + " must be a string");
            }
            properties.put(property.getKey(), property.getValue().textValue());
        });
        return Map.copyOf(properties);
    }

    static String sources(Set<SourceRef> sources) {
        ArrayNode array = MAPPER.createArrayNode();
        sources.stream()
                .sorted(Comparator.comparing(SourceRef::canonicalName))
                .forEach(source -> array.add(source(source)));
        return array.toString();
    }

    static Set<SourceRef> sources(JsonNode node) {
        if (node == null || !node.isArray()) {
            throw corrupt("definition sources must be a JSON array");
        }
        Set<SourceRef> sources = new TreeSet<>(Comparator.comparing(SourceRef::canonicalName));
        for (JsonNode entry : node) {
            sources.add(source(entry));
        }
        return sources;
    }

    static String policy(PolicySnapshot snapshot) {
        ObjectNode object = MAPPER.createObjectNode();
        snapshot.fingerprints().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(SourceRef::canonicalName)))
                .forEach(entry -> object.put(entry.getKey().canonicalName(), entry.getValue()));
        return object.toString();
    }

    static PolicySnapshot policy(JsonNode node) {
        if (node == null || !node.isObject()) {
            throw corrupt("definition policy snapshot must be a JSON object");
        }
        Map<SourceRef, String> fingerprints = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry ->
                fingerprints.put(sourceName(entry.getKey()), entry.getValue().asText()));
        return new PolicySnapshot(fingerprints);
    }

    static String outputSchema(OutputSchema schema) {
        ObjectNode object = MAPPER.createObjectNode();
        ArrayNode columns = object.putArray("columns");
        for (ColumnDefinition column : schema.columns()) {
            ObjectNode entry = columns.addObject();
            entry.put("name", column.name());
            entry.put("typeName", column.typeName());
            entry.put("nullable", column.nullable());
            entry.put("ordinal", column.ordinal());
        }
        return object.toString();
    }

    static OutputSchema outputSchema(JsonNode node) {
        if (node == null || !node.path("columns").isArray()) {
            throw corrupt("definition output schema must carry a columns array");
        }
        List<ColumnDefinition> columns = new ArrayList<>();
        for (JsonNode entry : node.path("columns")) {
            columns.add(new ColumnDefinition(
                    requiredText(entry, "name"),
                    requiredText(entry, "typeName"),
                    entry.path("nullable").asBoolean(),
                    entry.path("ordinal").asInt(-1)));
        }
        return new OutputSchema(columns);
    }

    static String sourceVector(SourceVector vector) {
        ObjectNode object = MAPPER.createObjectNode();
        object.put("canonicalVersion", vector.canonicalVersion());
        ArrayNode entries = object.putArray("entries");
        TreeMap<SourceRef, SourceSnapshotRef> ordered =
                new TreeMap<>(Comparator.comparing(SourceRef::canonicalName));
        ordered.putAll(vector.snapshots());
        ordered.forEach((source, snapshot) -> {
            ObjectNode entry = entries.addObject();
            entry.set("source", source(source));
            ObjectNode observed = entry.putObject("snapshot");
            observed.put("connectorId", snapshot.connectorId());
            observed.put("snapshotToken", snapshot.snapshotToken());
            observed.put("schemaFingerprint", snapshot.schemaFingerprint());
            observed.put("policyFingerprint", snapshot.policyFingerprint());
            observed.put("observedAt", snapshot.observedAt().toString());
            observed.put("pinnable", snapshot.pinnable());
        });
        return object.toString();
    }

    static SourceVector sourceVector(JsonNode node) {
        if (node == null || !node.path("entries").isArray() || !node.has("canonicalVersion")) {
            throw corrupt("source vector must carry canonicalVersion and an entries array");
        }
        TreeMap<SourceRef, SourceSnapshotRef> snapshots =
                new TreeMap<>(Comparator.comparing(SourceRef::canonicalName));
        for (JsonNode entry : node.path("entries")) {
            SourceRef source = source(entry.path("source"));
            JsonNode observed = entry.path("snapshot");
            SourceSnapshotRef snapshot = new SourceSnapshotRef(
                    requiredText(observed, "connectorId"),
                    requiredText(observed, "snapshotToken"),
                    requiredText(observed, "schemaFingerprint"),
                    requiredText(observed, "policyFingerprint"),
                    Instant.parse(requiredText(observed, "observedAt")),
                    observed.path("pinnable").asBoolean(false));
            if (snapshots.put(source, snapshot) != null) {
                throw corrupt("source vector contains duplicate source " + source.canonicalName());
            }
        }
        return new SourceVector(snapshots, node.path("canonicalVersion").asLong(-1));
    }

    private static ObjectNode source(SourceRef source) {
        ObjectNode object = MAPPER.createObjectNode();
        object.put("connectorId", source.connectorId());
        object.put("catalog", source.catalog());
        object.put("schema", source.schema());
        object.put("table", source.table());
        return object;
    }

    private static SourceRef source(JsonNode node) {
        return new SourceRef(
                requiredText(node, "connectorId"),
                requiredText(node, "catalog"),
                requiredText(node, "schema"),
                requiredText(node, "table"));
    }

    private static SourceRef sourceName(String canonicalName) {
        int colon = canonicalName.indexOf(':');
        int firstDot = canonicalName.indexOf('.', colon + 1);
        int secondDot = canonicalName.indexOf('.', firstDot + 1);
        if (colon <= 0 || firstDot <= colon || secondDot <= firstDot || secondDot == canonicalName.length() - 1) {
            throw corrupt("invalid source identity in policy snapshot: " + canonicalName);
        }
        return new SourceRef(
                canonicalName.substring(0, colon),
                canonicalName.substring(colon + 1, firstDot),
                canonicalName.substring(firstDot + 1, secondDot),
                canonicalName.substring(secondDot + 1));
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.asText().isBlank()) {
            throw corrupt("missing textual field " + field);
        }
        return value.asText();
    }

    static MaterializationException corrupt(String message) {
        return MaterializationException.repositoryFailure(message, null);
    }
}
