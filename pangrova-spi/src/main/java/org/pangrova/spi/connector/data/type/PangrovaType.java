package org.pangrova.spi.connector.data.type;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Enumeration of data types supported by the Pangrova type system.
 *
 * <p>These types serve as the canonical representation for Substrait type mapping.
 * Each value corresponds to a SQL type that can be mapped to and from
 * the SQL type system and Substrait {@code Type}.</p>
 */
@Experimental
public enum PangrovaType {

    /** Boolean value (true/false). */
    BOOLEAN,

    /** 8-bit signed integer. */
    TINYINT,

    /** 16-bit signed integer. */
    SMALLINT,

    /** 32-bit signed integer. */
    INTEGER,

    /** 64-bit signed integer. */
    BIGINT,

    /** 32-bit IEEE 754 floating point. */
    FLOAT,

    /** 64-bit IEEE 754 floating point. */
    DOUBLE,

    /** Arbitrary-precision decimal number. */
    DECIMAL,

    /** Calendar date (year, month, day). */
    DATE,

    /** Time of day without timezone. */
    TIME,

    /** Timestamp with microsecond precision (without timezone). */
    TIMESTAMP,

    /** Timestamp with microsecond precision (with timezone). */
    TIMESTAMP_TZ,

    /** Variable-length character string. */
    VARCHAR,

    /** Universally unique identifier (128-bit). */
    UUID,

    /** Fixed-length binary data. */
    FIXED,

    /** Variable-length binary data. */
    VARBINARY,

    /** Structured type with named fields. */
    STRUCT,

    /** Ordered collection of elements. */
    ARRAY,

    /** Key-value mapping. */
    MAP,

    /** Year-to-month interval for date arithmetic. */
    INTERVAL_YEAR_MONTH,

    /** Day-to-second interval for time arithmetic. */
    INTERVAL_DAY_TIME,

    /** Fixed-dimension floating-point vector for similarity search. */
    VECTOR,

    /** Semi-structured JSON type with shredded columnar encoding. */
    VARIANT,

    /** Geospatial geometry type stored as WKB binary. */
    GEOMETRY,

    /** Geospatial geography type (spherical coordinates) stored as WKB binary. */
    GEOGRAPHY,

    /** Timestamp with nanosecond precision (without timezone). */
    TIMESTAMP_NS,

    /** Placeholder type for columns whose type is not yet known. Queries return NULL. */
    UNKNOWN;

    /**
     * Parses a SQL type name string into the corresponding PangrovaType.
     * Handles common aliases (e.g. "INT" for INTEGER, "STRING" for VARCHAR)
     * and prefix matching for parameterized types (DECIMAL, NUMERIC).
     *
     * @param sqlTypeName the SQL type name (case-insensitive)
     * @return the corresponding PangrovaType
     * @throws IllegalArgumentException if the type name is not recognized
     */
    public static PangrovaType fromSqlTypeName(String sqlTypeName) {
        String upper = sqlTypeName.toUpperCase(java.util.Locale.ROOT);
        PangrovaType parameterizedType = parameterizedType(upper);
        if (parameterizedType != null) {
            return parameterizedType;
        }
        return switch (upper) {
            case "BOOLEAN" -> BOOLEAN;
            case "TINYINT" -> TINYINT;
            case "SMALLINT" -> SMALLINT;
            case "INT", "INTEGER" -> INTEGER;
            case "BIGINT" -> BIGINT;
            case "FLOAT" -> FLOAT;
            case "DOUBLE" -> DOUBLE;
            case "DATE" -> DATE;
            case "TIME" -> TIME;
            case "TIMESTAMP" -> TIMESTAMP;
            case "TIMESTAMP_TZ", "TIMESTAMPTZ" -> TIMESTAMP_TZ;
            case "VARCHAR", "STRING" -> VARCHAR;
            case "UUID" -> UUID;
            case "FIXED" -> FIXED;
            case "VARBINARY", "BINARY" -> VARBINARY;
            case "GEOMETRY" -> GEOMETRY;
            case "GEOGRAPHY" -> GEOGRAPHY;
            case "TIMESTAMP_NS" -> TIMESTAMP_NS;
            case "UNKNOWN" -> UNKNOWN;
            default -> throw new IllegalArgumentException(
                    "Unsupported SQL type name: " + sqlTypeName);
        };
    }

    private static PangrovaType parameterizedType(String typeName) {
        PangrovaType textualType = textualParameterizedType(typeName);
        if (textualType != null) {
            return textualType;
        }
        if (typeName.startsWith("VECTOR")) {
            return VECTOR;
        }
        if (typeName.startsWith("ARRAY")) {
            return ARRAY;
        }
        if (typeName.startsWith("VARIANT")) {
            return VARIANT;
        }
        return typeName.equals("FILE") ? STRUCT : null;
    }

    private static PangrovaType textualParameterizedType(String typeName) {
        if (typeName.startsWith("DECIMAL") || typeName.startsWith("NUMERIC")) {
            return DECIMAL;
        }
        if (typeName.startsWith("VARCHAR") || typeName.startsWith("STRING")) {
            return VARCHAR;
        }
        return null;
    }
}
