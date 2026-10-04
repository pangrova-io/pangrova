package org.pangrova.spi.coordination;

import java.util.Objects;

/**
 * Configuration for the Coordinator state journal.
 *
 * <p>{@code type} determines the journal implementation:
 * <ul>
 *   <li>{@code "NONE"} — instance-owned process-local journal, without persistence or HA</li>
 *   <li>{@code "JDBC"} — PostgreSQL-backed journal via {@code JdbcQueryStateJournal}</li>
 * </ul>
 *
 * @param type            journal backend type: "NONE" or "JDBC" (required)
 * @param jdbcUrl         JDBC connection URL (required when type is "JDBC")
 * @param flushIntervalMs async flush interval in milliseconds (default 100)
 * @param batchSize       max events per async flush batch (default 50)
 * @param retentionHours  hours to retain completed query events before purge (default 24)
 * @param maxResidentEvents maximum in-process retained events; not a byte or database-row limit
 */
public record StateJournalConfig(
        String type,
        String jdbcUrl,
        long flushIntervalMs,
        int batchSize,
        int retentionHours,
        int maxResidentEvents
) {
    /** Default configuration: no persistence. */
    public static final StateJournalConfig NONE =
            new StateJournalConfig("NONE", null, 100, 50, 24, 50_000);

    public StateJournalConfig {
        Objects.requireNonNull(type, "type must not be null");
        if (flushIntervalMs <= 0) {
            throw new IllegalArgumentException("flushIntervalMs must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (retentionHours <= 0) {
            throw new IllegalArgumentException("retentionHours must be positive");
        }
        if (maxResidentEvents <= 0) {
            throw new IllegalArgumentException("maxResidentEvents must be positive");
        }
    }
}
