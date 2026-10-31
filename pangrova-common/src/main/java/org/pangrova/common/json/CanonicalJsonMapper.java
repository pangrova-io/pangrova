package org.pangrova.common.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;

import java.io.IOException;

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

    /** Creates a new independently owned object node. */
    public static ObjectNode createObjectNode() {
        return INSTANCE.createObjectNode();
    }

    /** Creates a new independently owned array node. */
    public static ArrayNode createArrayNode() {
        return INSTANCE.createArrayNode();
    }
}
