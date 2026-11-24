package org.pangrova.common.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Canonical JSON serialization boundary shared by durable stores.
 *
 * <p>Delegates to {@link JsonMapperFactory} for configuration (JavaTimeModule,
 * ISO-8601 dates, lenient deserialization). Stores must use this boundary
 * instead of creating per-class ObjectMapper instances. The configured mapper
 * and its caches remain private and cannot be reconfigured by consumers.</p>
 */
public final class CanonicalJsonMapper {

    private static final ObjectMapper INSTANCE = JsonMapperFactory.create();

    private CanonicalJsonMapper() {}

    /** Serializes a value using the fixed canonical configuration. */
    public static <T> String writeValueAsString(T value) throws JsonProcessingException {
        return INSTANCE.writeValueAsString(value);
    }

    /** Writes JSON using the standard unchecked I/O exception type. */
    public static String writeValueAsStringUnchecked(Object value) {
        try {
            return writeValueAsString(value);
        } catch (JsonProcessingException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    /** Serializes canonical bytes without exposing mutable configuration. */
    public static <T> byte[] writeValueAsBytes(T value) throws JsonProcessingException {
        return INSTANCE.writeValueAsBytes(value);
    }

    /** Reads typed bytes using the fixed configuration. */
    public static <T> T readValue(byte[] bytes, Class<T> type) throws IOException {
        return INSTANCE.readValue(bytes, type);
    }

    /** Reads generic values using the fixed configuration. */
    public static <T> T readValue(String json, TypeReference<T> type) throws JsonProcessingException {
        return INSTANCE.readValue(json, type);
    }

    /** Reads an independently owned tree. */
    public static JsonNode readTree(String json) throws JsonProcessingException {
        return INSTANCE.readTree(json);
    }

    /** Returns the textual fields of a JSON object, or empty for invalid input. */
    public static Optional<Map<String, String>> readObjectTextFields(String json) {
        if (json == null) {
            return Optional.empty();
        }
        try {
            JsonNode object = readTree(json);
            if (object == null || !object.isObject()) {
                return Optional.empty();
            }
            Map<String, String> fields = new HashMap<>();
            object.fields().forEachRemaining(entry -> {
                if (entry.getValue().isTextual()) {
                    fields.put(entry.getKey(), entry.getValue().textValue());
                }
            });
            return Optional.of(Map.copyOf(fields));
        } catch (JsonProcessingException malformed) {
            return Optional.empty();
        }
    }

    /** Creates a new independently owned object node. */
    public static ObjectNode createObjectNode() {
        return INSTANCE.createObjectNode();
    }

    /** Creates a new independently owned array node. */
    public static ArrayNode createArrayNode() {
        return INSTANCE.createArrayNode();
    }
}
