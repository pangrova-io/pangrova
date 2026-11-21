package org.pangrova.streaming.format.json;

import com.fasterxml.jackson.databind.JsonNode;

import org.pangrova.streaming.api.cdc.CdcRow;

import java.util.LinkedHashMap;
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
    private static Object extractValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isNull()) {
            return null;
        }
        if (valueNode.isInt()) return valueNode.intValue();
        if (valueNode.isLong()) return valueNode.longValue();
        if (valueNode.isDouble() || valueNode.isFloat()) return valueNode.doubleValue();
        if (valueNode.isBoolean()) return valueNode.booleanValue();
        return valueNode.asText();
    }
}
