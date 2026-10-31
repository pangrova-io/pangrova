package org.pangrova.metadata.store.jdbc.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pangrova.common.json.CanonicalJsonMapper;

import java.io.IOException;

/**
 * Canonical JSON serialization boundary for metadata stores and definitions.
 *
 * <p>Delegates to {@link CanonicalJsonMapper} so metadata and other durable
 * owners share one fixed serialization configuration.</p>
 */
public final class MetadataJsonMapper {

    private MetadataJsonMapper() {}

    public static <T> String writeValueAsString(T value) throws JsonProcessingException {
        return CanonicalJsonMapper.writeValueAsString(value);
    }

    public static <T> byte[] writeValueAsBytes(T value) throws JsonProcessingException {
        return CanonicalJsonMapper.writeValueAsBytes(value);
    }

    public static <T> T readValue(byte[] bytes, Class<T> type) throws IOException {
        return CanonicalJsonMapper.readValue(bytes, type);
    }

    public static <T> T readValue(String json, TypeReference<T> type) throws JsonProcessingException {
        return CanonicalJsonMapper.readValue(json, type);
    }

    public static JsonNode readTree(String json) throws JsonProcessingException {
        return CanonicalJsonMapper.readTree(json);
    }

    public static ObjectNode createObjectNode() {
        return CanonicalJsonMapper.createObjectNode();
    }

    public static ArrayNode createArrayNode() {
        return CanonicalJsonMapper.createArrayNode();
    }
}
