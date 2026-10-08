package org.pangrova.parser.ast.type;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursive descent parser for canonical DataTypeSpec string representations.
 * Parses output of {@link DataTypeSpec#toCanonicalString()} back into a structurally
 * equal DataTypeSpec instance, enabling round-trip verification.
 *
 * <p>Supported formats:
 * <ul>
 *   <li>Primitive: {@code BIGINT}, {@code VARCHAR}, etc.</li>
 *   <li>Decimal: {@code DECIMAL(precision, scale)}</li>
 *   <li>Array: {@code ARRAY(elementType)}</li>
 *   <li>Map: {@code MAP(keyType, valueType)}</li>
 *   <li>Struct: {@code STRUCT(field1 type1, field2 type2, ...)}</li>
 *   <li>Vector: {@code VECTOR(dimension[, precision|metric[, metric]])}</li>
 * </ul>
 */
public final class DataTypeSpecParser {

    private static final int ERROR_CONTEXT_LENGTH = 20;

    private final String input;
    private int pos;

    private DataTypeSpecParser(String input) {
        this.input = input;
        this.pos = 0;
    }

    /**
     * Parses a canonical type string into a DataTypeSpec.
     *
     * @param canonical the canonical string produced by {@link DataTypeSpec#toCanonicalString()}
     * @return the parsed DataTypeSpec
     * @throws IllegalArgumentException if the input cannot be parsed
     */
    public static DataTypeSpec parse(String canonical) {
        if (canonical == null || canonical.isBlank()) {
            throw new IllegalArgumentException("canonical type string must not be null or blank");
        }
        DataTypeSpecParser parser = new DataTypeSpecParser(canonical.trim());
        DataTypeSpec result = parser.parseType();
        if (parser.pos != parser.input.length()) {
            throw new IllegalArgumentException(
                    "Unexpected trailing content at position " + parser.pos + ": '"
                            + parser.input.substring(parser.pos) + "'");
        }
        return result;
    }

    private DataTypeSpec parseType() {
        String token = peekToken();
        return switch (token) {
            case "ARRAY" -> parseArray();
            case "MAP" -> parseMap();
            case "STRUCT" -> parseStruct();
            case "DECIMAL" -> parseDecimal();
            case "VECTOR" -> parseVector();
            default -> parsePrimitive(token);
        };
    }

    private DataTypeSpec.ArrayType parseArray() {
        consumeToken("ARRAY");
        consumeChar('(');
        DataTypeSpec elementType = parseType();
        consumeChar(')');
        return new DataTypeSpec.ArrayType(elementType);
    }

    private DataTypeSpec.MapType parseMap() {
        consumeToken("MAP");
        consumeChar('(');
        DataTypeSpec keyType = parseType();
        consumeChar(',');
        skipWhitespace();
        DataTypeSpec valueType = parseType();
        consumeChar(')');
        return new DataTypeSpec.MapType(keyType, valueType);
    }

    private DataTypeSpec.StructType parseStruct() {
        consumeToken("STRUCT");
        consumeChar('(');
        List<FieldSpec> fields = new ArrayList<>();
        fields.add(parseField());
        while (peekChar() == ',') {
            consumeChar(',');
            skipWhitespace();
            fields.add(parseField());
        }
        consumeChar(')');
        return new DataTypeSpec.StructType(fields);
    }

    private FieldSpec parseField() {
        String name = parseIdentifier();
        skipWhitespace();
        DataTypeSpec type = parseType();
        return new FieldSpec(name, type);
    }

    private DataTypeSpec.DecimalType parseDecimal() {
        consumeToken("DECIMAL");
        consumeChar('(');
        int precision = parseInteger();
        consumeChar(',');
        skipWhitespace();
        int scale = parseInteger();
        consumeChar(')');
        return new DataTypeSpec.DecimalType(precision, scale);
    }

    private DataTypeSpec.VectorType parseVector() {
        consumeToken("VECTOR");
        consumeChar('(');
        int dimension = parseInteger();
        StringBuilder declaration = new StringBuilder("VECTOR(").append(dimension);
        while (peekChar() == ',') {
            consumeChar(',');
            declaration.append(',').append(parseIdentifier());
        }
        consumeChar(')');
        return (DataTypeSpec.VectorType) DataTypeSpec.fromRawTypeString(declaration.append(')').toString());
    }

    private DataTypeSpec.PrimitiveType parsePrimitive(String token) {
        consumeToken(token);
        BaseType baseType = BaseType.valueOf(token);
        return new DataTypeSpec.PrimitiveType(baseType);
    }

    private String peekToken() {
        skipWhitespace();
        int start = pos;
        while (pos < input.length() && isTokenChar(input.charAt(pos))) {
            pos++;
        }
        String token = input.substring(start, pos);
        pos = start;
        return token;
    }

    private void consumeToken(String expected) {
        skipWhitespace();
        if (!input.startsWith(expected, pos)) {
            throw new IllegalArgumentException(
                    "Expected '" + expected + "' at position " + pos
                            + " but found '" + safeSubstring(pos, ERROR_CONTEXT_LENGTH) + "'");
        }
        pos += expected.length();
    }

    private void consumeChar(char expected) {
        skipWhitespace();
        if (pos >= input.length() || input.charAt(pos) != expected) {
            char actual = pos < input.length() ? input.charAt(pos) : '\0';
            throw new IllegalArgumentException(
                    "Expected '" + expected + "' at position " + pos + " but found '" + actual + "'");
        }
        pos++;
    }

    private char peekChar() {
        skipWhitespace();
        return pos < input.length() ? input.charAt(pos) : '\0';
    }

    private int parseInteger() {
        skipWhitespace();
        int start = pos;
        while (pos < input.length() && Character.isDigit(input.charAt(pos))) {
            pos++;
        }
        if (start == pos) {
            throw new IllegalArgumentException("Expected integer at position " + pos);
        }
        String numStr = input.substring(start, pos);
        try {
            return Integer.parseInt(numStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid numeric literal '" + numStr + "' at position " + start, e);
        }
    }

    private String parseIdentifier() {
        skipWhitespace();
        int start = pos;
        while (pos < input.length() && isIdentifierChar(input.charAt(pos))) {
            pos++;
        }
        if (start == pos) {
            throw new IllegalArgumentException("Expected identifier at position " + pos);
        }
        return input.substring(start, pos);
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
            pos++;
        }
    }

    private String safeSubstring(int from, int maxLen) {
        return input.substring(from, Math.min(from + maxLen, input.length()));
    }

    private static boolean isTokenChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private static boolean isIdentifierChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}
