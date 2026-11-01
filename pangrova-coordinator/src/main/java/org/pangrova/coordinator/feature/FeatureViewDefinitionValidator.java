/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.feature;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.metadata.store.jdbc.MetadataJsonMapper;
import org.pangrova.coordinator.query.execution.AuthorizedRelation;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Validates the immutable single-source Feature View control-plane contract. */
public final class FeatureViewDefinitionValidator {

    private static final int MAX_DEFINITION_BYTES = 1_048_576;
    private static final Set<String> DEFINITION_FIELDS = Set.of(
            "entity_keys", "event_time", "source_table", "source_query", "transform_version",
            "schema", "freshness_target_ms", "lineage");

    /** Parses, validates and canonicalizes a Feature View definition. */
    public ValidatedFeatureViewDefinition validate(String definitionJson) {
        if (definitionJson == null || definitionJson.isBlank()
                || definitionJson.getBytes(StandardCharsets.UTF_8).length
                        > MAX_DEFINITION_BYTES) {
            throw invalid("Feature View definition must be non-empty and at most 1 MiB", null);
        }
        try {
            JsonNode root = MetadataJsonMapper.readTree(definitionJson);
            requireDefinitionFields(root);
            List<String> entityKeys = stringArray(root, "entity_keys");
            if (entityKeys.isEmpty() || new HashSet<>(entityKeys).size() != entityKeys.size()) {
                throw invalid("entity_keys must be non-empty and unique", null);
            }
            String eventTime = text(root, "event_time");
            QualifiedTableName source = qualifiedTable(text(root, "source_table"));
            String sourceQuery = text(root, "source_query");
            String transformVersion = text(root, "transform_version");
            requiredObject(root, "schema");
            requiredArray(root, "lineage");
            long freshness = root.path("freshness_target_ms").asLong(0);
            if (freshness <= 0) {
                throw invalid("freshness_target_ms must be positive", null);
            }
            JsonNode canonicalRoot = canonicalize(root);
            String canonical = MetadataJsonMapper.writeValueAsString(canonicalRoot);
            return new ValidatedFeatureViewDefinition(
                    canonical, DigestUtils.sha256Hex(canonical), entityKeys, eventTime,
                    source, sourceQuery, transformVersion,
                    canonicalRoot.get("schema").toString(), freshness, canonicalRoot.get("lineage").toString());
        } catch (JsonProcessingException failure) {
            throw invalid("Feature View definition is not valid JSON", failure);
        }
    }

    /** Validates real server-compiled outputs and the protected ordinary export graph. */
    public void validateCompiledSource(ValidatedFeatureViewDefinition definition, AuthorizedRelation relation) {
        Objects.requireNonNull(relation, "relation must not be null");
        if (relation.sources().size() != 1) {
            throw invalid("Feature View requires one exact declared source; "
                    + "joins require explicit source snapshot maps", null);
        }
        if (!relation.sources().get(0).qualifiedName().equals(definition.sourceTable().toString())) {
            throw invalid("Compiled source_query must read exactly its declared source_table", null);
        }
        try {
            FeatureExportStageGraph.requireAdmitted(relation.stageGraph(), definition.sourceTable().toString());
        } catch (IllegalArgumentException unsupported) {
            throw invalid(unsupported.getMessage(), unsupported);
        }
        requireOutputNames(relation);
        try {
            JsonNode schema = MetadataJsonMapper.readTree(definition.schemaJson());
            Set<String> names = new HashSet<>();
            schema.fieldNames().forEachRemaining(names::add);
            requireOutputTypes(relation, schema, names);
            requireEntityAndEvent(definition, schema, names);
        } catch (JsonProcessingException failure) {
            throw invalid("Feature View schema is not valid JSON", failure);
        }
    }

    private static void requireDefinitionFields(JsonNode root) {
        if (root == null || !root.isObject()) {
            throw invalid("Feature View definition must be a JSON object", null);
        }
        root.fieldNames().forEachRemaining(field -> {
            if (!DEFINITION_FIELDS.contains(field)) {
                throw invalid("Unsupported Feature View field '" + field
                        + "'; execution plans are produced only by the authenticated SQL compiler", null);
            }
        });
    }

    private static void requireOutputNames(AuthorizedRelation relation) {
        if (relation.outputNames().isEmpty()
                || new HashSet<>(relation.outputNames()).size() != relation.outputNames().size()
                || relation.outputNames().stream().anyMatch(String::isBlank)
                || relation.outputNames().size() != relation.outputTypeNames().size()) {
            throw invalid("Compiled Feature View outputs must have unique names and exact types", null);
        }
    }

    private static void requireOutputTypes(AuthorizedRelation relation, JsonNode schema, Set<String> names) {
        if (!names.equals(Set.copyOf(relation.outputNames()))) {
            throw invalid("schema fields must exactly match compiled source_query outputs", null);
        }
        for (int ordinal = 0; ordinal < relation.outputNames().size(); ordinal++) {
            String name = relation.outputNames().get(ordinal);
            PangrovaType actual = scalarType(relation.outputTypeNames().get(ordinal));
            JsonNode declared = schema.get(name);
            if (!declared.isTextual() || scalarType(declared.textValue()) != actual) {
                throw invalid("Declared schema differs from compiled output type for " + name, null);
            }
        }
    }

    private static void requireEntityAndEvent(ValidatedFeatureViewDefinition definition,
        JsonNode schema, Set<String> names) {
        for (String key : definition.entityKeys()) {
            if (!names.contains(key)) {
                throw invalid("entity_keys must identify compiled output columns: " + key, null);
            }
        }
        if (!names.contains(definition.eventTimeColumn())
                || scalarType(schema.get(definition.eventTimeColumn()).textValue()) != PangrovaType.TIMESTAMP) {
            throw invalid("event_time must identify a compiled microsecond TIMESTAMP output column", null);
        }
    }

    private static PangrovaType scalarType(String name) {
        PangrovaType type;
        try {
            type = PangrovaType.fromSqlTypeName(name);
        } catch (RuntimeException failure) {
            throw invalid("Unsupported Feature View scalar output type: " + name, failure);
        }
        return switch (type) {
            case BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR,
                    DATE, TIME, TIMESTAMP, VARBINARY -> type;
            default -> throw invalid("Feature View output requires an exact supported scalar type: " + name, null);
        };
    }

    private static QualifiedTableName qualifiedTable(String value) {
        String[] parts = value.split("\\.", -1);
        if (parts.length != 3 || java.util.Arrays.stream(parts).anyMatch(String::isBlank)) {
            throw invalid("source_table must be catalog.schema.table", null);
        }
        return new QualifiedTableName(parts[0], parts[1], parts[2]);
    }

    private static String text(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw invalid("Feature View field '" + field + "' must be non-blank text", null);
        }
        return value.textValue();
    }

    private static List<String> stringArray(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isArray()) {
            throw invalid("Feature View field '" + field + "' must be an array", null);
        }
        List<String> result = new ArrayList<>();
        value.forEach(item -> {
            if (!item.isTextual() || item.textValue().isBlank()) {
                throw invalid("Feature View field '" + field + "' contains a blank", null);
            }
            result.add(item.textValue());
        });
        return List.copyOf(result);
    }

    private static JsonNode requiredObject(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isObject() || value.isEmpty()) {
            throw invalid("Feature View field '" + field + "' must be a non-empty object", null);
        }
        return value;
    }

    private static JsonNode requiredArray(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isArray() || value.isEmpty()) {
            throw invalid("Feature View field '" + field + "' must be a non-empty array", null);
        }
        return value;
    }

    private static JsonNode canonicalize(JsonNode node) {
        if (node.isObject()) {
            ObjectNode sorted = MetadataJsonMapper.createObjectNode();
            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);
            names.stream().sorted(Comparator.naturalOrder())
                    .forEach(name -> sorted.set(name, canonicalize(node.get(name))));
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode values = MetadataJsonMapper.createArrayNode();
            node.forEach(value -> values.add(canonicalize(value)));
            return values;
        }
        return node;
    }

    private static PangrovaException invalid(String message, Throwable cause) {
        return cause == null
                ? new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message)
                : new PangrovaException(
                        PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message, cause);
    }

    /** Validated fields persisted independently of the caller's JSON formatting. */
    public record ValidatedFeatureViewDefinition(
            String canonicalJson,
            String sha256,
            List<String> entityKeys,
            String eventTimeColumn,
            QualifiedTableName sourceTable,
            String sourceQuery,
            String transformVersion,
            String schemaJson,
            long freshnessTargetMillis,
            String lineageJson) {

        public ValidatedFeatureViewDefinition {
            Objects.requireNonNull(canonicalJson, "canonicalJson must not be null");
            entityKeys = List.copyOf(entityKeys);
        }
    }
}
