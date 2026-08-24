package org.pangrova.streaming.cdc.format;

import com.fasterxml.jackson.databind.JsonNode;

import org.pangrova.spi.streaming.CdcRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared JSON-to-CdcRow parsing utilities used by all CDC format converters.
 *
 * <p>Stateless and thread-safe. All methods are static.</p>
 */
public final class CdcRowParser {

    private CdcRowParser() {}

    /**
     * Converts a JSON object node to a {@link CdcRow}.
     *
     * @param node JSON object representing a row, or null
     * @return CdcRow if node is a non-null object, null otherwise
     */
    public static CdcRow toCdcRow(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        Map<String, Object> columns = new LinkedHashMap<>();
        node.fields().forEachRemaining(
                entry -> columns.put(entry.getKey(), extractValue(entry.getValue())));
        return CdcRow.of(columns);
    }

    /**
     * Extracts a typed Java value from a JSON value node.
     *
     * @param valueNode JSON value node
     * @return typed Java value (Integer, Long, Double, Boolean, String, or null)
     */
    public static Object extractValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isNull()) {
            return null;
        }
        if (valueNode.isInt()) return valueNode.intValue();
        if (valueNode.isLong()) return valueNode.longValue();
        if (valueNode.isDouble() || valueNode.isFloat()) return valueNode.doubleValue();
        if (valueNode.isBoolean()) return valueNode.booleanValue();
        return valueNode.asText();
    }

    /**
     * Extracts schema version from a Debezium-style payload.
     *
     * <p>Strategy: prefer {@code source.struct_version} (explicit schema version).
     * Fall back to a stable hash of sorted field names from {@code after} or {@code before}.</p>
     *
     * <p>Note: {@code source.version} is the Debezium <em>connector</em> version
     * (e.g., "2.4.0"), NOT the schema version. Parsing it as int always fails.</p>
     *
     * @param payload the Debezium payload node
     * @return schema version integer (0 if undetermined)
     */
    public static int extractSchemaVersion(JsonNode payload) {
        int structVersion = tryExtractStructVersion(payload);
        if (structVersion >= 0) {
            return structVersion;
        }
        return computeFieldNameHash(payload);
    }

    private static int tryExtractStructVersion(JsonNode payload) {
        JsonNode source = payload.get("source");
        if (source != null && source.has("struct_version")) {
            try {
                return Integer.parseInt(source.get("struct_version").asText());
            } catch (NumberFormatException ignored) {
                // fall through to field name hash
            }
        }
        return -1;
    }

    private static int computeFieldNameHash(JsonNode payload) {
        JsonNode fields = payload.get("after");
        if (fields == null || fields.isNull()) {
            fields = payload.get("before");
        }
        if (fields != null && fields.isObject()) {
            List<String> names = new ArrayList<>();
            fields.fieldNames().forEachRemaining(names::add);
            Collections.sort(names);
            return names.hashCode();
        }
        return 0;
    }
}
