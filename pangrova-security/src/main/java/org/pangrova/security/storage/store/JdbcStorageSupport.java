package org.pangrova.security.storage.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static java.util.Objects.requireNonNull;

/**
 * Shared JDBC helpers for the security-owned file space and storage profile stores.
 *
 * <p>Mirrors the canonical PostgreSQL binding rules of the metadata store layer so
 * timestamp equality and compare-and-set values stay stable across the JDBC round
 * trip without security depending on metadata.</p>
 */
public final class JdbcStorageSupport {

    private JdbcStorageSupport() {
    }

    /**
     * Canonicalizes an instant to PostgreSQL {@code TIMESTAMPTZ(6)} precision.
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

    @FunctionalInterface
    public interface SqlFunction<T, R> {
        R apply(T input) throws SQLException;
    }
}
