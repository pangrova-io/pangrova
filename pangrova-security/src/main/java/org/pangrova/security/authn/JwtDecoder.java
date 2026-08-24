package org.pangrova.security.authn;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Decodes JWT segments and parses JSON claims without external library dependency.
 *
 * <p>Handles flat and nested JSON with string, number, boolean, null, array,
 * and object values. Sufficient for OIDC discovery documents and JWT payloads.</p>
 */
final class JwtDecoder {

    /** Maximum JSON nesting depth to prevent stack overflow from malicious payloads. */
    private static final int MAX_NESTING_DEPTH = 10;

    private JwtDecoder() {}

    /**
     * Decodes a JWT segment (0=header, 1=payload) from a compact JWS token.
     */
    static Map<String, Object> decodeSegment(String token, int segmentIndex) {
        String[] parts = token.split("\\.");
        if (parts.length < 2) {
            throw new OAuth2Authenticator.TokenValidationException(
                    "Invalid JWT format: expected at least 2 segments");
        }
        if (segmentIndex >= parts.length) {
            throw new OAuth2Authenticator.TokenValidationException(
                    "JWT segment index out of bounds");
        }
        try {
            byte[] decoded = Base64.getUrlDecoder().decode(parts[segmentIndex]);
            String json = new String(decoded, StandardCharsets.UTF_8);
            return parseJsonClaims(json);
        } catch (IllegalArgumentException e) {
            throw new OAuth2Authenticator.TokenValidationException(
                    "Invalid JWT encoding in segment " + segmentIndex);
        }
    }

    /**
     * Minimal JSON parser for JWT claims. Handles flat key-value pairs
     * with string, number, boolean, null, and simple array values.
     * Avoids external JSON library dependency in the security module.
     */
    static Map<String, Object> parseJsonClaims(String json) {
        return parseJsonClaims(json, 0);
    }

    private static Map<String, Object> parseJsonClaims(String json, int depth) {
        if (depth > MAX_NESTING_DEPTH) {
            throw new OAuth2Authenticator.TokenValidationException(
                    "JSON nesting depth exceeds maximum of " + MAX_NESTING_DEPTH);
        }
        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) {
            throw new OAuth2Authenticator.TokenValidationException("Invalid JSON claims");
        }
        String inner = json.substring(1, json.length() - 1).trim();
        if (inner.isEmpty()) {
            return Map.of();
        }
        java.util.Map<String, Object> result = new java.util.LinkedHashMap<>();
        int[] pos = {0};
        while (pos[0] < inner.length()) {
            if (!parseEntry(inner, pos, result, depth)) break;
        }
        return Map.copyOf(result);
    }

    /** Parses one key-value entry from the JSON body, advancing pos. Returns false on malformed input. */
    private static boolean parseEntry(String json, int[] pos, java.util.Map<String, Object> result, int depth) {
        skipDelimiters(json, pos);
        if (pos[0] >= json.length()) return false;
        String key = parseKey(json, pos);
        if (key == null) return false;
        skipColon(json, pos);
        if (pos[0] >= json.length()) return false;
        result.put(key, parseValue(json, pos, depth));
        return true;
    }

    private static void skipDelimiters(String json, int[] pos) {
        while (pos[0] < json.length()
                && (json.charAt(pos[0]) == ',' || Character.isWhitespace(json.charAt(pos[0])))) {
            pos[0]++;
        }
    }

    private static String parseKey(String json, int[] pos) {
        if (json.charAt(pos[0]) != '"') return null;
        int keyEnd = json.indexOf('"', pos[0] + 1);
        if (keyEnd < 0) return null;
        String key = json.substring(pos[0] + 1, keyEnd);
        pos[0] = keyEnd + 1;
        return key;
    }

    private static void skipColon(String json, int[] pos) {
        while (pos[0] < json.length()
                && (json.charAt(pos[0]) == ':' || Character.isWhitespace(json.charAt(pos[0])))) {
            pos[0]++;
        }
    }

    private static Object parseValue(String json, int[] pos, int depth) {
        char c = json.charAt(pos[0]);
        return switch (c) {
            case '"' -> parseStringValue(json, pos);
            case '[' -> parseArrayValue(json, pos, depth);
            case '{' -> parseObjectValue(json, pos, depth);
            case 't', 'f' -> parseBooleanValue(json, pos);
            case 'n' -> {
                pos[0] += 4;
                yield null;
            }
            default -> parseNumberValue(json, pos);
        };
    }

    private static String parseStringValue(String json, int[] pos) {
        int valEnd = findClosingQuote(json, pos[0] + 1);
        String value = (valEnd < 0) ? "" : json.substring(pos[0] + 1, valEnd);
        pos[0] = (valEnd < 0) ? json.length() : valEnd + 1;
        return value;
    }

    private static List<Object> parseArrayValue(String json, int[] pos, int depth) {
        int bracketEnd = findClosingBracket(json, pos[0]);
        if (bracketEnd < 0) {
            pos[0] = json.length();
            return List.of();
        }
        String arrContent = json.substring(pos[0] + 1, bracketEnd).trim();
        pos[0] = bracketEnd + 1;
        return parseArray(arrContent, depth);
    }

    private static Map<String, Object> parseObjectValue(String json, int[] pos, int depth) {
        int braceEnd = findClosingBrace(json, pos[0]);
        if (braceEnd < 0) {
            pos[0] = json.length();
            return Map.of();
        }
        Map<String, Object> nested = parseJsonClaims(json.substring(pos[0], braceEnd + 1), depth + 1);
        pos[0] = braceEnd + 1;
        return nested;
    }

    private static Boolean parseBooleanValue(String json, int[] pos) {
        boolean val = json.startsWith("true", pos[0]);
        pos[0] += val ? 4 : 5;
        return val;
    }

    private static Number parseNumberValue(String json, int[] pos) {
        int numEnd = pos[0];
        while (numEnd < json.length() && isNumericChar(json.charAt(numEnd))) {
            numEnd++;
        }
        String numStr = json.substring(pos[0], numEnd);
        pos[0] = numEnd;
        return numStr.contains(".") ? Double.parseDouble(numStr) : Long.parseLong(numStr);
    }

    private static boolean isNumericChar(char c) {
        return Character.isDigit(c) || c == '.' || c == '-';
    }

    private static List<Object> parseArray(String arrContent, int depth) {
        if (arrContent.isEmpty()) {
            return new java.util.ArrayList<>();
        }
        return arrContent.startsWith("{")
                ? parseObjectArray(arrContent, depth)
                : parseSimpleArray(arrContent);
    }

    private static List<Object> parseObjectArray(String content, int depth) {
        java.util.List<Object> items = new java.util.ArrayList<>();
        int pos = 0;
        while (pos < content.length()) {
            pos = skipWhitespaceAndCommas(content, pos);
            if (pos >= content.length() || content.charAt(pos) != '{') break;
            int objEnd = findClosingBrace(content, pos);
            if (objEnd < 0) break;
            items.add(parseJsonClaims(content.substring(pos, objEnd + 1), depth + 1));
            pos = objEnd + 1;
        }
        return items;
    }

    private static List<Object> parseSimpleArray(String content) {
        java.util.List<Object> items = new java.util.ArrayList<>();
        for (String item : content.split(",")) {
            item = item.trim();
            if (item.startsWith("\"") && item.endsWith("\"")) {
                item = item.substring(1, item.length() - 1);
            }
            items.add(item);
        }
        return items;
    }

    private static int skipWhitespaceAndCommas(String s, int pos) {
        while (pos < s.length()
                && (s.charAt(pos) == ',' || Character.isWhitespace(s.charAt(pos)))) {
            pos++;
        }
        return pos;
    }

    static int findClosingQuote(String json, int start) {
        for (int i = start; i < json.length(); i++) {
            if (json.charAt(i) == '"' && (i == start || json.charAt(i - 1) != '\\')) {
                return i;
            }
        }
        return -1;
    }

    static int findClosingBracket(String json, int start) {
        int depth = 0;
        for (int i = start; i < json.length(); i++) {
            if (json.charAt(i) == '[') depth++;
            else if (json.charAt(i) == ']') {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }

    static int findClosingBrace(String json, int start) {
        int depth = 0;
        boolean inString = false;
        for (int i = start; i < json.length(); i++) {
            char ch = json.charAt(i);
            if (ch == '"' && (i == start || json.charAt(i - 1) != '\\')) {
                inString = !inString;
                continue;
            }
            if (inString) {
                continue;
            }
            depth += braceDepthDelta(ch);
            if (depth == 0) return i;
        }
        return -1;
    }

    private static int braceDepthDelta(char ch) {
        if (ch == '{') return 1;
        if (ch == '}') return -1;
        return 0;
    }
}
