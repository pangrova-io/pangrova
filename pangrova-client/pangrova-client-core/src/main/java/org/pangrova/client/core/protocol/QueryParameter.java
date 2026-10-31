package org.pangrova.client.core.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A typed query parameter for parameterized query submission.
 *
 * <p>Parameters are sent alongside the SQL template (which retains {@code ?} placeholders)
 * so the server can perform type-safe binding without lossy client-side string interpolation.
 *
 * <p>The {@code value} is always a string-serialized representation using canonical formats:
 * <ul>
 *   <li>INTEGER, BIGINT, SMALLINT, TINYINT: decimal integer string (e.g. "42")</li>
 *   <li>DOUBLE, FLOAT, REAL: decimal floating-point (e.g. "3.14")</li>
 *   <li>DECIMAL: plain decimal without scientific notation (e.g. "123.456")</li>
 *   <li>BOOLEAN: "true" or "false"</li>
 *   <li>VARCHAR, CHAR: the raw string value (no SQL quoting)</li>
 *   <li>DATE: ISO-8601 date "yyyy-MM-dd"</li>
 *   <li>TIME: ISO-8601 time "HH:mm:ss"</li>
 *   <li>TIMESTAMP: ISO-8601 UTC instant "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"</li>
 *   <li>NULL: value is null</li>
 * </ul>
 */
public record QueryParameter(
    @JsonProperty("type") String type,
    @JsonProperty("value") String value
) {

    @JsonCreator
    public QueryParameter {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Parameter type must not be null or blank");
        }
        // value may be null for NULL parameters
    }

    /** Creates a NULL parameter of the given SQL type. */
    public static QueryParameter ofNull(String type) {
        return new QueryParameter(type, null);
    }

    /** Creates a VARCHAR parameter. */
    public static QueryParameter ofVarchar(String value) {
        return new QueryParameter("VARCHAR", value);
    }

    /** Creates an INTEGER parameter. */
    public static QueryParameter ofInteger(int value) {
        return new QueryParameter("INTEGER", Integer.toString(value));
    }

    /** Creates a BIGINT parameter. */
    public static QueryParameter ofBigint(long value) {
        return new QueryParameter("BIGINT", Long.toString(value));
    }

    /** Creates a DOUBLE parameter. */
    public static QueryParameter ofDouble(double value) {
        return new QueryParameter("DOUBLE", Double.toString(value));
    }

    /** Creates a REAL (float) parameter. */
    public static QueryParameter ofReal(float value) {
        return new QueryParameter("REAL", Float.toString(value));
    }

    /** Creates a DECIMAL parameter. */
    public static QueryParameter ofDecimal(java.math.BigDecimal value) {
        return new QueryParameter("DECIMAL", value.toPlainString());
    }

    /** Creates a BOOLEAN parameter. */
    public static QueryParameter ofBoolean(boolean value) {
        return new QueryParameter("BOOLEAN", Boolean.toString(value));
    }

    /** Creates a DATE parameter (ISO-8601 format). */
    public static QueryParameter ofDate(java.sql.Date value) {
        return new QueryParameter("DATE", value.toLocalDate().toString());
    }

    /** Creates a TIME parameter (ISO-8601 format). */
    public static QueryParameter ofTime(java.sql.Time value) {
        return new QueryParameter("TIME", value.toLocalTime().toString());
    }

    /**
     * Creates a TIMESTAMP parameter in UTC ISO-8601 format.
     * The timestamp is converted to UTC to avoid timezone ambiguity.
     */
    public static QueryParameter ofTimestamp(java.sql.Timestamp value) {
        java.time.Instant instant = value.toInstant();
        String formatted = java.time.format.DateTimeFormatter.ISO_INSTANT.format(instant);
        return new QueryParameter("TIMESTAMP", formatted);
    }
}
