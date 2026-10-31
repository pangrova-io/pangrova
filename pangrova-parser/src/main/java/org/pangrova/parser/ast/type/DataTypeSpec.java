package org.pangrova.parser.ast.type;

import org.pangrova.connector.spi.data.type.VectorType.ElementType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Sealed type hierarchy representing parsed SQL column type definitions.
 * Replaces raw String type representation in AST nodes, enabling downstream
 * validators and planners to work with structured types without re-parsing.
 */
public sealed interface DataTypeSpec permits
        DataTypeSpec.PrimitiveType,
        DataTypeSpec.DecimalType,
        DataTypeSpec.ArrayType,
        DataTypeSpec.MapType,
        DataTypeSpec.StructType,
        DataTypeSpec.VectorType,
        DataTypeSpec.FileType {

    int DEFAULT_DECIMAL_PRECISION = 38;

    /**
     * Returns a canonical string representation suitable for display, debugging,
     * and round-trip parsing (e.g. "MAP(VARCHAR, ARRAY(INT))").
     */
    String toCanonicalString();

    /**
     * Parses a raw SQL type string into a structured DataTypeSpec.
     * Handles primitive types, DECIMAL(p,s), ARRAY(...), MAP(...,...),
     * STRUCT(...), and VECTOR(dim[, precision|metric[, metric]]).
     *
     * @param rawType the uppercase SQL type string from the grammar
     * @return the corresponding DataTypeSpec
     * @throws IllegalArgumentException if the type string is not recognized
     */
    static DataTypeSpec fromRawTypeString(String rawType) {
        Objects.requireNonNull(rawType, "rawType must not be null");
        String trimmed = rawType.trim();
        String upper = trimmed.toUpperCase();

        if (upper.startsWith("DECIMAL(") || upper.startsWith("NUMERIC(")) {
            return parseDecimal(upper);
        }
        if (upper.equals("DECIMAL") || upper.equals("NUMERIC")) {
            return new DecimalType(DEFAULT_DECIMAL_PRECISION, 0);
        }
        if (upper.startsWith("ARRAY(")) {
            String inner = extractInnerContent(trimmed, "ARRAY");
            return new ArrayType(fromRawTypeString(inner));
        }
        if (upper.startsWith("MAP(")) {
            String inner = extractInnerContent(trimmed, "MAP");
            int splitIdx = findTopLevelComma(inner);
            String keyStr = inner.substring(0, splitIdx).trim();
            String valStr = inner.substring(splitIdx + 1).trim();
            return new MapType(fromRawTypeString(keyStr), fromRawTypeString(valStr));
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
        return new FieldSpec(name, fromRawTypeString(typeStr));
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

    /**
     * A primitive scalar type (e.g. BIGINT, VARCHAR, BOOLEAN).
     */
    record PrimitiveType(BaseType baseType) implements DataTypeSpec {

        public PrimitiveType {
            Objects.requireNonNull(baseType, "baseType must not be null");
        }

        @Override
        public String toCanonicalString() {
            return baseType.name();
        }
    }

    /**
     * A fixed-precision decimal type with explicit precision and scale.
     */
    record DecimalType(int precision, int scale) implements DataTypeSpec {

        public DecimalType {
            if (precision <= 0) {
                throw new IllegalArgumentException("precision must be positive, got " + precision);
            }
            if (scale < 0) {
                throw new IllegalArgumentException("scale must be non-negative, got " + scale);
            }
            if (scale > precision) {
                throw new IllegalArgumentException(
                        "scale (" + scale + ") must not exceed precision (" + precision + ")");
            }
        }

        @Override
        public String toCanonicalString() {
            return "DECIMAL(" + precision + ", " + scale + ")";
        }
    }

    /**
     * An array type containing elements of a specified type.
     */
    record ArrayType(DataTypeSpec elementType) implements DataTypeSpec {

        public ArrayType {
            Objects.requireNonNull(elementType, "elementType must not be null");
        }

        @Override
        public String toCanonicalString() {
            return "ARRAY(" + elementType.toCanonicalString() + ")";
        }
    }

    /**
     * A map type with specified key and value types.
     */
    record MapType(DataTypeSpec keyType, DataTypeSpec valueType) implements DataTypeSpec {

        public MapType {
            Objects.requireNonNull(keyType, "keyType must not be null");
            Objects.requireNonNull(valueType, "valueType must not be null");
        }

        @Override
        public String toCanonicalString() {
            return "MAP(" + keyType.toCanonicalString() + ", " + valueType.toCanonicalString() + ")";
        }
    }

    /**
     * A struct (row) type composed of named fields.
     */
    record StructType(List<FieldSpec> fields) implements DataTypeSpec {

        public StructType {
            Objects.requireNonNull(fields, "fields must not be null");
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("struct must have at least one field");
            }
            fields = List.copyOf(fields);
        }

        @Override
        public String toCanonicalString() {
            String fieldList = fields.stream()
                    .map(FieldSpec::toCanonicalString)
                    .collect(Collectors.joining(", "));
            return "STRUCT(" + fieldList + ")";
        }
    }

    /**
     * A fixed-dimension storage declaration with coordinate precision and legacy similarity metadata.
     * Executable LogicalType canonicalization belongs to the protocol contract, not this neutral DTO.
     */
    record VectorType(int dimension, DistanceMetric metric, ElementType elementType) implements DataTypeSpec {

        /** Preserves existing metric declarations with their declared default FLOAT32 storage. */
        public VectorType(int dimension, DistanceMetric metric) {
            this(dimension, metric, ElementType.FLOAT);
        }

        public VectorType {
            Objects.requireNonNull(metric, "metric must not be null");
            Objects.requireNonNull(elementType, "elementType must not be null");
            // The SPI parameterized type owns the storage dimension bounds.
            new org.pangrova.connector.spi.data.type.VectorType(dimension, elementType);
        }

        @Override
        public String toCanonicalString() {
            if (metric == DistanceMetric.COSINE) {
                return "VECTOR(" + dimension + (elementType == ElementType.DOUBLE ? ",DOUBLE" : "") + ")";
            }
            // Retain non-default legacy metric metadata; it is not executable LogicalType authority.
            return "VECTOR(" + dimension + ", "
                    + (elementType == ElementType.DOUBLE ? "DOUBLE, " : "") + metric.name() + ")";
        }
    }

    /**
     * The FILE type — a syntactic sugar for a fixed STRUCT schema that stores
     * multimodal file references using the Claim-Check pattern.
     * Physical schema: STRUCT(uri VARCHAR, mime_type VARCHAR, size BIGINT, checksum VARCHAR, created_at TIMESTAMP).
     *
     * <p>The storage modifier decides whether the engine copies the referenced bytes into
     * managed storage ({@code FILE MANAGED}) or records a reference to the caller's object
     * ({@code FILE EXTERNAL}, the default for plain {@code FILE}). Both keep the same five
     * physical fields; version identity stays on the referenced object.</p>
     */
    record FileType(FileStorage storage) implements DataTypeSpec {

        /** How file bytes relate to the row that references them. */
        public enum FileStorage {
            EXTERNAL,
            MANAGED
        }

        public FileType {
            Objects.requireNonNull(storage, "storage must not be null");
        }

        /** Plain {@code FILE}: a reference to an existing object. */
        public static final FileType INSTANCE = new FileType(FileStorage.EXTERNAL);

        /** {@code FILE MANAGED}: bytes are copied into managed storage on write. */
        public static final FileType MANAGED = new FileType(FileStorage.MANAGED);

        @Override
        public String toCanonicalString() {
            return storage == FileStorage.MANAGED ? "FILE MANAGED" : "FILE";
        }
    }
}
