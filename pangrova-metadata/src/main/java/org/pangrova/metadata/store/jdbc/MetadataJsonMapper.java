package org.pangrova.metadata.store.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.pangrova.common.json.JsonMapperFactory;

import java.io.IOException;

/**
 * Canonical JSON serialization boundary for metadata stores and definitions.
 *
 * <p>Delegates to {@link JsonMapperFactory} for configuration (JavaTimeModule,
 * ISO-8601 dates, lenient deserialization). All metadata store classes should
 * use this boundary instead of creating per-class ObjectMapper instances.
 * The configured mapper and its caches remain private and cannot be reconfigured by consumers.</p>
 */
public final class MetadataJsonMapper {

    private static final ObjectMapper INSTANCE = JsonMapperFactory.create();

    private MetadataJsonMapper() {}

    /**
     * Serializes a value using the fixed metadata configuration.
     */
    public static <T> String writeValueAsString(T value) throws JsonProcessingException {
        return INSTANCE.writeValueAsString(value);
    }

    /** Serializes canonical metadata bytes without exposing mutable configuration. */
    public static <T> byte[] writeValueAsBytes(T value) throws JsonProcessingException {
        return INSTANCE.writeValueAsBytes(value);
    }

    /** Reads typed metadata bytes using the fixed configuration. */
    public static <T> T readValue(byte[] bytes, Class<T> type) throws IOException {
        return INSTANCE.readValue(bytes, type);
    }

    /** Reads generic metadata values using the fixed configuration. */
    public static <T> T readValue(String json, TypeReference<T> type) throws JsonProcessingException {
        return INSTANCE.readValue(json, type);
    }

    /** Reads an independently owned definition tree. */
    public static JsonNode readTree(String json) throws JsonProcessingException {
        return INSTANCE.readTree(json);
    }

    /** Creates a new independently owned definition object. */
    public static ObjectNode createObjectNode() {
        return INSTANCE.createObjectNode();
    }

    /** Creates a new independently owned definition array. */
    public static ArrayNode createArrayNode() {
        return INSTANCE.createArrayNode();
    }
}
