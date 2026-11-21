package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Bounded strict JSON support for the sole source-controlled binding declaration. */
final class BindingDeclarationJson {
    private static final int MAX_BYTES = 1048576;
    private static final int MAX_DEPTH = 64;
    private static final int MAX_PRINTABLE_ASCII = 126;
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private BindingDeclarationJson() {}

    static JsonNode load() {
        String path = "/function-contracts/sql-bindings.json";
        try (InputStream stream = BindingDeclarationJson.class.getResourceAsStream(path)) {
            if (stream == null) {
                throw new IllegalStateException("Canonical binding declaration is absent: " + path);
            }
            byte[] bytes = stream.readNBytes(MAX_BYTES + 1);
            if (bytes.length > MAX_BYTES) {
                throw new IllegalArgumentException("Binding declaration exceeds its byte bound");
            }
            JsonNode result = MAPPER.readTree(bytes);
            canonical(result, 0);
            return result;
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot read canonical binding declaration", failure);
        }
    }

    static byte[] canonicalBytes(JsonNode value) {
        try {
            return MAPPER.writeValueAsBytes(canonical(value, 0));
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot encode binding declaration", failure);
        }
    }

    private static JsonNode canonical(JsonNode value, int depth) {
        if (value == null || depth > MAX_DEPTH) {
            throw new IllegalArgumentException("Binding declaration exceeds its depth bound");
        }
        if (value.isObject()) {
            ObjectNode result = MAPPER.createObjectNode();
            List<String> names = new ArrayList<>();
            value.fieldNames().forEachRemaining(names::add);
            names.stream().sorted().forEach(name -> result.set(name, canonical(value.get(name), depth + 1)));
            return result;
        }
        if (value.isArray()) {
            ArrayNode result = MAPPER.createArrayNode();
            value.forEach(element -> result.add(canonical(element, depth + 1)));
            return result;
        }
        if (value.isTextual()) {
            String text = value.textValue();
            if (text.chars().anyMatch(character -> character < 32 || character > MAX_PRINTABLE_ASCII)) {
                throw new IllegalArgumentException("Binding declarations require printable ASCII text");
            }
        } else if (!value.isBoolean() && !value.isIntegralNumber()) {
            throw new IllegalArgumentException("Binding declarations permit only exact integers and booleans");
        }
        return value;
    }

    static void keys(JsonNode node, Set<String> required, Set<String> optional) {
        if (!node.isObject()) {
            throw new IllegalArgumentException("Binding declaration entry must be an object");
        }
        Set<String> actual = new HashSet<>();
        node.fieldNames().forEachRemaining(actual::add);
        Set<String> allowed = new HashSet<>(required);
        allowed.addAll(optional);
        if (!actual.containsAll(required) || !allowed.containsAll(actual)) {
            throw new IllegalArgumentException("Missing or unknown binding declaration fields: " + actual);
        }
    }

    static String text(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || !value.isTextual() || value.textValue().isEmpty()) {
            throw new IllegalArgumentException("Binding declaration requires a nonempty string: " + name);
        }
        return value.textValue();
    }
}
