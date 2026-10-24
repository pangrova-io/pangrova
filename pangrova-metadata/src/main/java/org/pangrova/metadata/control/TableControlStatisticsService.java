package org.pangrova.metadata.control;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Persists statistics collected by {@code ANALYZE TABLE} for controlled tables.
 *
 * <p>Only LINKED and MANAGED records are eligible: an OBSERVED table has no stable
 * identity, so its statistics could not be tied to the table they describe. Every
 * row is bound to the control record's version token; once the table advances the
 * row stops matching and readers fall back to live connector statistics.</p>
 */
public final class TableControlStatisticsService {

    private final TableControlService control;
    private final TableControlStatisticsStore store;
    private final Clock clock;

    public TableControlStatisticsService(
            TableControlService control, TableControlStatisticsStore store) {
        this(control, store, Clock.systemUTC());
    }

    public TableControlStatisticsService(TableControlService control,
            TableControlStatisticsStore store, Clock clock) {
        this.control = Objects.requireNonNull(control, "control");
        this.store = Objects.requireNonNull(store, "store");
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    /**
     * Records one collection result, binding it to the control record's version.
     *
     * @throws AccessDeniedException if the table is OBSERVED or not ACTIVE
     * @throws PangrovaException     if the table has no version token to bind to,
     *                               or the connector returned no usable statistics
     */
    public TableControlStatistics record(String catalog, String schema, String table,
            TableStatistics statistics, String source) {
        return record(catalog, schema, table, statistics, source, null);
    }

    /**
     * Records one collection result for a controlled table, binding it to the
     * version token the collector actually observed.
     *
     * <p>When the observed token differs from the control record, the record is
     * advanced first (as an observation, never as a write intent), so the newly
     * collected statistics are not born stale. The advance must be provable: an
     * identity change, a retired record, or a concurrent advance past the
     * collected version rejects the result instead of persisting statistics
     * against a version the collector never saw. A blank observed token falls
     * back to the recorded one; if neither exists the collection is rejected.</p>
     *
     * @throws AccessDeniedException if the table is OBSERVED or not ACTIVE
     * @throws PangrovaException     if there is no version token to bind to,
     *                               the control record cannot absorb the observed
     *                               version, or the connector returned no usable
     *                               statistics
     */
    public TableControlStatistics record(String catalog, String schema, String table,
            TableStatistics statistics, String source, String observedVersionToken) {
        Objects.requireNonNull(statistics, "statistics must not be null");
        TableControlRecord record = requireActiveControlled(catalog, schema, table);
        String versionToken = observedVersionToken == null || observedVersionToken.isBlank()
                ? record.versionToken() : observedVersionToken;
        if (versionToken == null || versionToken.isBlank()) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "ANALYZE rejected: " + record.qualifiedName() + " has no version token, "
                            + "so collected statistics cannot be bound to a table version");
        }
        if (!versionToken.equals(record.versionToken())) {
            ObservationOutcome outcome = control.observeVersion(record, Optional.empty(),
                    Optional.of(new org.pangrova.spi.connector.metadata.table.TableVersionToken(
                            record.format(), versionToken)),
                    source, clock.instant());
            if (outcome != ObservationOutcome.VERSION_CHANGED
                    && outcome != ObservationOutcome.UNCHANGED) {
                throw new AccessDeniedException("ANALYZE rejected: the control record for "
                        + record.qualifiedName() + " could not accept the observed version "
                        + "(" + outcome + "); re-check the table identity before collecting");
            }
            record = requireActiveControlled(catalog, schema, table);
            if (!versionToken.equals(record.versionToken())) {
                throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "ANALYZE rejected: " + record.qualifiedName()
                                + " advanced past the collected version ("
                                + versionToken + " -> " + record.versionToken()
                                + "); retry when writers are idle");
            }
        }
        if (statistics.rowCount() < 0 && statistics.dataSize() < 0) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "ANALYZE rejected: the connector returned no statistics for "
                            + record.qualifiedName());
        }
        long statsVersion = store.find(record.tableId())
                .map(existing -> existing.statsVersion() + 1)
                .orElse(0L);
        TableControlStatistics collected = new TableControlStatistics(record.tableId(),
                statsVersion, versionToken, statistics, source, clock.instant());
        store.save(collected);
        return collected;
    }

    /**
     * Returns persisted statistics only while they still describe the table's
     * current version. A missing record or a changed token yields empty, so the
     * caller reads live connector statistics instead.
     */
    public Optional<TableControlStatistics> fresh(String catalog, String schema, String table) {
        if (!control.enabled()) {
            return Optional.empty();
        }
        return control.find(catalog, schema, table).flatMap(this::fresh);
    }

    /** Freshness check for a caller that already holds the control record. */
    public Optional<TableControlStatistics> fresh(TableControlRecord record) {
        if (record == null) {
            return Optional.empty();
        }
        return store.find(record.tableId())
                .filter(stats -> stats.matchesVersion(record.versionToken()));
    }

    /** Removes statistics for a table that was unlinked or dropped. */
    public void purge(UUID tableId) {
        store.delete(tableId);
    }

    private TableControlRecord requireActiveControlled(
            String catalog, String schema, String table) {
        if (!control.enabled()) {
            throw new AccessDeniedException("ANALYZE requires the table control plane: "
                    + "without ownership records no table can be proven safe to analyze");
        }
        TableControlRecord record = control.find(catalog, schema, table)
                .orElseThrow(() -> new AccessDeniedException("ANALYZE rejected: "
                        + catalog + "." + schema + "." + table
                        + " is OBSERVED; run ALTER TABLE ... ADOPT before ANALYZE"));
        if (record.ownershipMode() == OwnershipMode.OBSERVED) {
            throw new AccessDeniedException(
                    "ANALYZE rejected: " + record.qualifiedName() + " is OBSERVED");
        }
        if (record.state() != TableControlState.ACTIVE) {
            throw new AccessDeniedException("ANALYZE rejected: the control record for "
                    + record.qualifiedName() + " is " + record.state()
                    + "; ANALYZE requires ACTIVE");
        }
        return record;
    }
}
