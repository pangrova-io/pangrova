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
package org.pangrova.semantic.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Validates and canonicalizes the metadata-only Semantic View definition contract. */
public final class SemanticViewDefinitionValidator {

    private static final ObjectMapper MAPPER = JsonMapperFactory.shared();

    static final int MAX_DEFINITION_BYTES = 1_048_576;
    private static final Set<String> ARRAY_FIELDS = Set.of(
            "relationships", "dimensions", "facts", "measures", "filters");

    /** Validates a definition and returns canonical content plus referenced source tables. */
    public ValidatedSemanticViewDefinition validate(String definitionJson) {
        if (definitionJson == null || definitionJson.isBlank()) {
            throw invalid("Semantic View definition must not be blank", null);
        }
        if (definitionJson.getBytes(StandardCharsets.UTF_8).length > MAX_DEFINITION_BYTES) {
            throw invalid("Semantic View definition exceeds 1 MiB", null);
        }
        try {
            JsonNode root = MAPPER.readTree(definitionJson);
            if (root == null || !root.isObject()) {
                throw invalid("Semantic View definition must be a JSON object", null);
            }
            List<QualifiedTableName> sources = validateLogicalTables(root);
            validateOptionalArrays(root);
            String canonicalJson = MAPPER.writeValueAsString(canonicalize(root));
            return new ValidatedSemanticViewDefinition(
                    canonicalJson, Sha256Digests.sha256Hex(canonicalJson), sources);
        } catch (JsonProcessingException failure) {
            throw invalid("Semantic View definition is not valid JSON", failure);
        }
    }

    private static List<QualifiedTableName> validateLogicalTables(JsonNode root) {
        JsonNode tables = root.get("logicalTables");
        if (tables == null || !tables.isArray() || tables.isEmpty()) {
            throw invalid("Semantic View definition requires logicalTables", null);
        }
        List<QualifiedTableName> sources = new ArrayList<>();
        for (JsonNode table : tables) {
            if (!table.isObject() || !hasText(table, "name") || !hasText(table, "source")) {
                throw invalid("Each logical table requires name and source", null);
            }
            String[] parts = table.get("source").textValue().split("\\.", -1);
            if (parts.length != 3 || java.util.Arrays.stream(parts).anyMatch(String::isBlank)) {
                throw invalid("Logical table source must be catalog.schema.table", null);
            }
            sources.add(new QualifiedTableName(parts[0], parts[1], parts[2]));
        }
        return List.copyOf(sources);
    }

    private static void validateOptionalArrays(JsonNode root) {
        for (String field : ARRAY_FIELDS) {
            JsonNode value = root.get(field);
            if (value != null && !value.isArray()) {
                throw invalid("Semantic View field '" + field + "' must be an array", null);
            }
        }
    }

    private static boolean hasText(JsonNode object, String field) {
        JsonNode value = object.get(field);
        return value != null && value.isTextual() && !value.textValue().isBlank();
    }

    private static JsonNode canonicalize(JsonNode node) {
        if (node.isObject()) {
            ObjectNode sorted = MAPPER.createObjectNode();
            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);
            names.stream().sorted(Comparator.naturalOrder())
                    .forEach(name -> sorted.set(name, canonicalize(node.get(name))));
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode array = MAPPER.createArrayNode();
            node.forEach(value -> array.add(canonicalize(value)));
            return array;
        }
        return node;
    }

    private static PangrovaException invalid(String message, Throwable cause) {
        return cause == null
                ? new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message)
                : new PangrovaException(
                        PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message, cause);
    }

    /** Canonical definition persisted by the control plane. */
    public record ValidatedSemanticViewDefinition(
            String canonicalJson,
            String sha256,
            List<QualifiedTableName> sources) {

        public ValidatedSemanticViewDefinition {
            Objects.requireNonNull(canonicalJson, "canonicalJson must not be null");
            Objects.requireNonNull(sha256, "sha256 must not be null");
            Objects.requireNonNull(sources, "sources must not be null");
            sources = List.copyOf(sources);
        }
    }
}
