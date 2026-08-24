package org.pangrova.indexing.engine.gin;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts elements from composite values (arrays, JSON-like strings)
 * for GIN index construction.
 */
final class ElementExtractor {

    private ElementExtractor() {}

    /**
     * Extracts string elements from a composite value.
     * Supports comma-separated values, JSON-like arrays, and plain strings.
     *
     * @param value the composite value
     * @return list of extracted element strings
     */
    static List<String> extract(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        String trimmed = value.strip();

        // JSON-like array: [elem1, elem2, ...]
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            return parseArray(trimmed.substring(1, trimmed.length() - 1));
        }

        // Comma-separated
        if (trimmed.contains(",")) {
            return parseArray(trimmed);
        }

        // Single element
        return List.of(trimmed);
    }

    private static List<String> parseArray(String content) {
        List<String> elements = new ArrayList<>();
        for (String part : content.split(",")) {
            String cleaned = part.strip();
            // Remove surrounding quotes
            if (cleaned.length() >= 2
                    && ((cleaned.startsWith("\"") && cleaned.endsWith("\""))
                    || (cleaned.startsWith("'") && cleaned.endsWith("'")))) {
                cleaned = cleaned.substring(1, cleaned.length() - 1);
            }
            if (!cleaned.isEmpty()) {
                elements.add(cleaned);
            }
        }
        return elements;
    }
}
