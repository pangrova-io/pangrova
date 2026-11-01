package org.pangrova.table.api.statistics;

import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Statistics collected by {@code ANALYZE TABLE}, bound to the stable table and the
 * version token that was current when they were collected.
 *
 * @param tableId      stable identity of the controlled table
 * @param statsVersion monotonically increasing per-table collection counter
 * @param versionToken table version the statistics describe
 * @param statistics   collected connector statistics
 * @param source       collection source marker (for example {@code ANALYZE})
 * @param computedAt   collection time
 */
public record TableControlStatistics(
        UUID tableId,
        long statsVersion,
        String versionToken,
        TableStatistics statistics,
        String source,
        Instant computedAt) {

    public TableControlStatistics {
        Objects.requireNonNull(tableId, "tableId");
        if (statsVersion < 0) {
            throw new IllegalArgumentException("statsVersion must not be negative");
        }
        if (versionToken == null || versionToken.isBlank()) {
            throw new IllegalArgumentException("versionToken must not be blank");
        }
        Objects.requireNonNull(statistics, "statistics");
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source must not be blank");
        }
        Objects.requireNonNull(computedAt, "computedAt");
    }

    /**
     * Whether these statistics still describe the table's current version. A missing
     * or changed token means the table advanced (or the connector stopped reporting
     * one), so the statistics must not be used.
     */
    public boolean matchesVersion(String currentVersionToken) {
        return currentVersionToken != null && !currentVersionToken.isBlank()
                && versionToken.equals(currentVersionToken);
    }
}
