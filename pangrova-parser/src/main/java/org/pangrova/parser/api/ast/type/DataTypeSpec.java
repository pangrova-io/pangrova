package org.pangrova.parser.api.ast.type;

import org.pangrova.parser.api.enums.FileStorageMode;
import org.pangrova.parser.api.enums.BaseType;
import org.pangrova.parser.api.enums.DistanceMetric;
import org.pangrova.types.enums.VectorElementType;

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
    record VectorType(int dimension, DistanceMetric metric, VectorElementType elementType) implements DataTypeSpec {

        /** Preserves existing metric declarations with their declared default FLOAT32 storage. */
        public VectorType(int dimension, DistanceMetric metric) {
            this(dimension, metric, VectorElementType.FLOAT);
        }

        public VectorType {
            Objects.requireNonNull(metric, "metric must not be null");
            Objects.requireNonNull(elementType, "elementType must not be null");
            // The SPI parameterized type owns the storage dimension bounds.
            new org.pangrova.types.VectorType(dimension, elementType);
        }

        @Override
        public String toCanonicalString() {
            if (metric == DistanceMetric.COSINE) {
                return "VECTOR(" + dimension + (elementType == VectorElementType.DOUBLE ? ",DOUBLE" : "") + ")";
            }
            // Retain non-default legacy metric metadata; it is not executable LogicalType authority.
            return "VECTOR(" + dimension + ", "
                    + (elementType == VectorElementType.DOUBLE ? "DOUBLE, " : "") + metric.name() + ")";
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
    record FileType(FileStorageMode storage) implements DataTypeSpec {

        public FileType {
            Objects.requireNonNull(storage, "storage must not be null");
        }

        /** Plain {@code FILE}: a reference to an existing object. */
        public static final FileType INSTANCE = new FileType(FileStorageMode.EXTERNAL);

        /** {@code FILE MANAGED}: bytes are copied into managed storage on write. */
        public static final FileType MANAGED = new FileType(FileStorageMode.MANAGED);

        @Override
        public String toCanonicalString() {
            return storage == FileStorageMode.MANAGED ? "FILE MANAGED" : "FILE";
        }
    }
}
