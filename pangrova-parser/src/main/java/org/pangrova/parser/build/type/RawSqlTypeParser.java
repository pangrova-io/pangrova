package org.pangrova.parser.build.type;

import java.util.List;
import java.util.Objects;
import org.pangrova.types.VectorType.ElementType;
import org.pangrova.parser.api.enums.BaseType;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.parser.api.enums.DistanceMetric;
import org.pangrova.parser.api.ast.type.FieldSpec;
import org.pangrova.parser.api.ast.type.DataTypeSpec.ArrayType;
import org.pangrova.parser.api.ast.type.DataTypeSpec.DecimalType;
import org.pangrova.parser.api.ast.type.DataTypeSpec.FileType;
import org.pangrova.parser.api.ast.type.DataTypeSpec.MapType;
import org.pangrova.parser.api.ast.type.DataTypeSpec.PrimitiveType;
import org.pangrova.parser.api.ast.type.DataTypeSpec.StructType;
import org.pangrova.parser.api.ast.type.DataTypeSpec.VectorType;

/** Parses legacy raw SQL column declarations; value validation belongs to AST values. */
public final class RawSqlTypeParser {
    private RawSqlTypeParser() {}
    /**
     * Parses a raw SQL type string into a structured DataTypeSpec.
     * Handles primitive types, DECIMAL(p,s), ARRAY(...), MAP(...,...),
     * STRUCT(...), and VECTOR(dim[, precision|metric[, metric]]).
     *
     * @param rawType the uppercase SQL type string from the grammar
     * @return the corresponding DataTypeSpec
     * @throws IllegalArgumentException if the type string is not recognized
     */
    public static DataTypeSpec parse(String rawType) {
        Objects.requireNonNull(rawType, "rawType must not be null");
        String trimmed = rawType.trim();
        String upper = trimmed.toUpperCase();

        if (upper.startsWith("DECIMAL(") || upper.startsWith("NUMERIC(")) {
            return parseDecimal(upper);
        }
        if (upper.equals("DECIMAL") || upper.equals("NUMERIC")) {
            return new DecimalType(DataTypeSpec.DEFAULT_DECIMAL_PRECISION, 0);
        }
        if (upper.startsWith("ARRAY(")) {
            String inner = extractInnerContent(trimmed, "ARRAY");
            return new ArrayType(parse(inner));
        }
        if (upper.startsWith("MAP(")) {
            String inner = extractInnerContent(trimmed, "MAP");
            int splitIdx = findTopLevelComma(inner);
            String keyStr = inner.substring(0, splitIdx).trim();
            String valStr = inner.substring(splitIdx + 1).trim();
            return new MapType(parse(keyStr), parse(valStr));
        }
        if (upper.startsWith("STRUCT(")) {
            String inner = extractInnerContent(trimmed, "STRUCT");
            List<FieldSpec> fields = parseStructFields(inner);
            return new StructType(fields);
        }
        if (upper.startsWith("VECTOR(")) {
            return parseVector(upper);
        }
        if (upper.equals("FILE") || upper.equals("FILE EXTERNAL")) {
            return FileType.INSTANCE;
        }
        if (upper.equals("FILE MANAGED")) {
            return FileType.MANAGED;
        }
        BaseType resolved = resolveBaseType(upper);
        if (resolved != null) {
            return new PrimitiveType(resolved);
        }
        throw new IllegalArgumentException("Unrecognized SQL type: " + rawType);
    }

    /**
     * Resolves a raw type name to a BaseType, handling common SQL aliases.
     */
    private static BaseType resolveBaseType(String name) {
        return switch (name) {
            case "INT" -> BaseType.INTEGER;
            default -> {
                try {
                    yield BaseType.valueOf(name);
                } catch (IllegalArgumentException e) {
                    yield null;
                }
            }
        };
    }

    private static DecimalType parseDecimal(String s) {
        String prefix = s.startsWith("NUMERIC(") ? "NUMERIC" : "DECIMAL";
        String inner = extractInnerContent(s, prefix);
        String[] parts = inner.split(",");
        int precision;
        try {
            precision = Integer.parseInt(parts[0].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid numeric literal for precision: '" + parts[0].trim() + "' in type: " + s, e);
        }
        int scale;
        try {
            scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid numeric literal for scale: '" + parts[1].trim() + "' in type: " + s, e);
        }
        return new DecimalType(precision, scale);
    }

    private static VectorType parseVector(String s) {
        String inner = extractInnerContent(s, "VECTOR");
        String[] parts = inner.split(",", -1);
        int dimension;
        try {
            dimension = Integer.parseInt(parts[0].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid numeric literal for vector dimension: '" + parts[0].trim() + "' in type: " + s, e);
        }
        if (parts.length > 3) {
            throw new IllegalArgumentException("VECTOR accepts dimension, precision and optional metric: " + s);
        }
        ElementType element = ElementType.FLOAT;
        DistanceMetric metric = DistanceMetric.COSINE;
        if (parts.length > 1) {
            String parameter = parts[1].trim();
            if (parameter.equals("FLOAT") || parameter.equals("DOUBLE")) {
                element = ElementType.valueOf(parameter);
                if (parts.length == 3) metric = DistanceMetric.valueOf(parts[2].trim());
            } else {
                if (parts.length == 3) throw new IllegalArgumentException("Invalid VECTOR precision: " + parameter);
                metric = DistanceMetric.valueOf(parameter);
            }
        }
        return new VectorType(dimension, metric, element);
    }

    private static List<FieldSpec> parseStructFields(String inner) {
        List<FieldSpec> fields = new java.util.ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                fields.add(parseOneField(inner.substring(start, i).trim()));
                start = i + 1;
            }
        }
        if (start < inner.length()) {
            fields.add(parseOneField(inner.substring(start).trim()));
        }
        return List.copyOf(fields);
    }

    private static FieldSpec parseOneField(String fieldStr) {
        int spaceIdx = fieldStr.indexOf(' ');
        if (spaceIdx < 0) {
            throw new IllegalArgumentException("Invalid STRUCT field (expected 'name type'): " + fieldStr);
        }
        String name = fieldStr.substring(0, spaceIdx).trim();
        String typeStr = fieldStr.substring(spaceIdx + 1).trim();
        return new FieldSpec(name, parse(typeStr));
    }

    private static String extractInnerContent(String s, String prefix) {
        int open = prefix.length();
        if (s.charAt(open) != '(') {
            throw new IllegalArgumentException("Expected '(' after " + prefix + " in: " + s);
        }
        if (s.charAt(s.length() - 1) != ')') {
            throw new IllegalArgumentException("Expected closing ')' in: " + s);
        }
        return s.substring(open + 1, s.length() - 1);
    }

    private static int findTopLevelComma(String s) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                return i;
            }
        }
        throw new IllegalArgumentException("No top-level comma found in MAP type: " + s);
    }

}
