package org.pangrova.indexing.store.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static java.util.Objects.requireNonNull;

/** Shared JDBC bind/read primitives for the indexing JDBC stores. */
public final class JdbcIndexSupport {

    private JdbcIndexSupport() {}

    /** Binds canonical PostgreSQL JSON/JSONB text without implicit VARCHAR casts. */
    public static void setJson(PreparedStatement statement, int index, String json)
            throws SQLException {
        statement.setObject(index, json, Types.OTHER);
    }

    /**
     * Canonicalizes an instant to PostgreSQL {@code TIMESTAMPTZ(6)} precision so
     * equality and compare-and-set values stay stable across the JDBC round trip.
     */
    public static Instant canonicalInstant(Instant value) {
        return requireNonNull(value, "value is null").truncatedTo(ChronoUnit.MICROS);
    }

    /** Binds an absolute instant as an explicit UTC {@link OffsetDateTime}. */
    public static void setInstant(PreparedStatement statement, int index, Instant value)
            throws SQLException {
        statement.setObject(index, canonicalInstant(value).atOffset(ZoneOffset.UTC));
    }

    /** Reads a nullable time-zone-aware timestamp and returns its absolute instant. */
    public static Instant getInstant(ResultSet result, String column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : canonicalInstant(value.toInstant());
    }

    /** Reads a nullable time-zone-aware timestamp and returns its absolute instant. */
    public static Instant getInstant(ResultSet result, int column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : canonicalInstant(value.toInstant());
    }

    /** Truncates a failure or staleness reason to the persisted column bound. */
    public static String truncateReason(String reason) {
        String effective = reason == null || reason.isBlank() ? "state_transition" : reason;
        return effective.substring(0, Math.min(effective.length(), 1024));
    }
}
