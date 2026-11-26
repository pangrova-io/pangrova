package org.pangrova.planner.statistics;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.connector.api.metadata.table.TableStatistics.ColumnStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects and manages table/column statistics for CBO.
 * Supports manual ANALYZE, auto-on-write incremental, periodic scheduled,
 * file-level footer reads, histogram auto-collection, and confidence scoring.
 */
public class StatisticsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(StatisticsCollector.class);

    static final double LOW_CARDINALITY_RATIO = 0.01;

    private final Map<QualifiedTableName, PlannerTableStatistics> store =
            new ConcurrentHashMap<>();
    private final ConnectorStatisticsProvider connectorProvider;

    /** Creates a collector with a real connector statistics provider. */
    public StatisticsCollector(ConnectorStatisticsProvider connectorProvider) {
        this.connectorProvider = connectorProvider;
    }

    /** Creates a collector with no connector provider (standalone/test mode). */
    public StatisticsCollector() {
        this(null);
    }

    /** Req 7.1 — Full ANALYZE TABLE collection. */
    public PlannerTableStatistics analyzeTable(
            QualifiedTableName tableName, List<String> columns) {
        Map<String, org.pangrova.planner.api.statistics.ColumnStatistics> colStats = new HashMap<>();
        // Try real connector statistics first
        if (connectorProvider != null) {
            org.pangrova.connector.api.metadata.table.TableStatistics spiStats =
                    connectorProvider.statistics(tableName);
            if (spiStats != null && spiStats.rowCount() >= 0) {
                for (String col : columns) {
                    ColumnStatistics spiCol = spiStats.columnStats().get(col);
                    colStats.put(col, convertFromSpi(spiCol, spiStats.rowCount()));
                }
                PlannerTableStatistics stats =
                        new PlannerTableStatistics(tableName, colStats,
                                Instant.now());
                store.put(tableName, stats);
                return stats;
            }
        }
        // Fallback: collect from file footers or use defaults
        for (String col : columns) {
            colStats.put(col, collectFullColumn(col));
        }
        PlannerTableStatistics stats =
                new PlannerTableStatistics(tableName, colStats, Instant.now());
        store.put(tableName, stats);
        return stats;
    }

    /** Req 7.2 — Lightweight incremental collection after write commit. */
    public PlannerTableStatistics collectIncremental(
            QualifiedTableName tableName, long affectedRows) {
        PlannerTableStatistics existing = store.get(tableName);
        if (existing == null) {
            return analyzeTable(tableName, List.of());
        }
        double sampleRate = adaptiveSampleRate(affectedRows);
        Map<String, org.pangrova.planner.api.statistics.ColumnStatistics> merged =
                mergeIncremental(existing.columns(), sampleRate);
        PlannerTableStatistics updated =
                new PlannerTableStatistics(tableName, merged, Instant.now());
        store.put(tableName, updated);
        return updated;
    }

    /** Req 7.3 — Staleness detection for periodic scheduler. */
    public boolean checkStaleness(QualifiedTableName tableName, Instant currentSnapshot, Duration staleThreshold) {
        PlannerTableStatistics existing = store.get(tableName);
        if (existing == null) {
            return true;
        }
        Duration age = Duration.between(existing.lastUpdatedSnapshot(), currentSnapshot);
        return age.compareTo(staleThreshold) > 0;
    }

    /** Req 7.5 — Read file-level column statistics from Iceberg/Parquet footers. */
    public org.pangrova.planner.api.statistics.ColumnStatistics readFileFooterStats(
            String columnName, long rowCount, long nullCount, Comparable<?> min, Comparable<?> max) {
        return new org.pangrova.planner.api.statistics.ColumnStatistics(
                rowCount, nullCount, min, max, 0L, 0.0, Optional.empty(), 1.0);
    }

    /** Req 7.6 — Decide whether to auto-collect histogram. */
    public boolean shouldCollectHistogram(String columnName, long ndv, long rowCount,
                                          boolean isJoinKey, boolean isFilterColumn) {
        if (isJoinKey || isFilterColumn) {
            return true;
        }
        return rowCount > 0 && ndv < (long) (rowCount * LOW_CARDINALITY_RATIO);
    }

    /** Req 7.7 — Confidence score based on staleness and change rate. */
    public double computeConfidence(Duration staleness, double changeRate) {
        double hours = staleness.toMillis() / 3_600_000.0;
        double raw = 1.0 - hours * changeRate;
        return Math.max(0.1, Math.min(1.0, raw));
    }

    /** Retrieve stored statistics for a table, lazily loading from connector if available. */
    public Optional<PlannerTableStatistics> getStatistics(QualifiedTableName tableName) {
        PlannerTableStatistics cached = store.get(tableName);
        if (cached != null) {
            return Optional.of(cached);
        }
        // Lazy load from connector provider when stats are not in store
        if (connectorProvider != null) {
            try {
                org.pangrova.connector.api.metadata.table.TableStatistics spiStats =
                        connectorProvider.statistics(tableName);
                if (spiStats != null && spiStats.rowCount() >= 0) {
                    Map<String, org.pangrova.planner.api.statistics.ColumnStatistics> colStats = new HashMap<>();
                    for (var entry : spiStats.columnStats().entrySet()) {
                        colStats.put(entry.getKey(), convertFromSpi(entry.getValue(), spiStats.rowCount()));
                    }
                    PlannerTableStatistics stats =
                            new PlannerTableStatistics(tableName,
                                    colStats, Instant.now());
                    store.put(tableName, stats);
                    return Optional.of(stats);
                }
            } catch (org.pangrova.common.exception.PangrovaException authorityFailure) {
                throw authorityFailure;
            } catch (Exception e) {
                LOG.warn("event=connector_statistics_load_failed table={} reason={}", tableName, e.getMessage());
            }
        }
        return Optional.empty();
    }

    /** Store pre-built statistics for a table. */
    public void putStatistics(QualifiedTableName tableName,
            PlannerTableStatistics stats) {
        java.util.Objects.requireNonNull(tableName, "tableName");
        java.util.Objects.requireNonNull(stats, "stats");
        if (!tableName.equals(stats.tableName())) {
            throw new IllegalArgumentException("Statistics identity differs from the cache key");
        }
        store.put(tableName, stats);
    }

    /**
     * Records runtime cardinality feedback from adaptive query execution.
     *
     * <p>When the actual row count observed after a shuffle stage differs significantly
     * from the estimated row count, this method adjusts the stored statistics to
     * improve future cost estimates. The adjustment uses exponential smoothing:
     * {@code newRowCount = alpha * actual + (1 - alpha) * estimated}, where alpha
     * is derived from the divergence ratio.</p>
     *
     * <p>Only updates existing statistics — does not create entries for unknown tables.
     * Thread-safe: relies on ConcurrentHashMap's compute-if-present atomicity.</p>
     *
     * @param tableName     fully qualified table name
     * @param estimatedRows the planner's original row count estimate
     * @param actualRows    the actual row count observed at runtime
     */
    public void recordRuntimeFeedback(QualifiedTableName tableName, long estimatedRows, long actualRows) {
        if (estimatedRows <= 0 || actualRows <= 0) {
            return;
        }
        double ratio = (double) actualRows / estimatedRows;
        if (ratio >= 0.5 && ratio <= 2.0) {
            return;
        }
        double alpha = Math.min(0.7, Math.abs(ratio - 1.0) / (Math.abs(ratio - 1.0) + 1.0));
        long adjustedRows = (long) (alpha * actualRows + (1.0 - alpha) * estimatedRows);

        store.computeIfPresent(tableName, (key, existing) -> {
            Map<String, org.pangrova.planner.api.statistics.ColumnStatistics> adjusted = new HashMap<>();
            for (var entry : existing.columns().entrySet()) {
                org.pangrova.planner.api.statistics.ColumnStatistics old = entry.getValue();
                // Confidence reflects accuracy of the adjusted value:
                // mild divergence (2-5x) → slight boost (we corrected it)
                // severe divergence (>5x) → decrease (data is volatile)
                double newConfidence;
                double absRatio = Math.max(ratio, 1.0 / ratio);
                if (absRatio <= 5.0) {
                    newConfidence = Math.min(1.0, old.confidenceScore() + 0.05);
                } else {
                    newConfidence = Math.max(0.3, old.confidenceScore() - 0.1);
                }
                adjusted.put(entry.getKey(), new org.pangrova.planner.api.statistics.ColumnStatistics(
                        adjustedRows, old.nullCount(), old.min(), old.max(),
                        old.ndv(), old.avgSize(), old.histogram(), newConfidence));
            }
            LOG.info("event=runtime_feedback_applied table={} estimated={} actual={} adjusted={} alpha={}",
                    tableName, estimatedRows, actualRows, adjustedRows,
                    String.format("%.2f", alpha));
            return new PlannerTableStatistics(
                    tableName, adjusted, Instant.now());
        });
    }

    /** Invalidates all cached statistics. */
    public void invalidateAll() {
        store.clear();
        LOG.info("event=statistics_invalidate_all");
    }

    /** Lets the query source share the lookup for two uncached join inputs. */
    public void prepareJoinStatistics(QualifiedTableName left, QualifiedTableName right) {
        if (connectorProvider != null && !left.equals(right) && !store.containsKey(left) && !store.containsKey(right)) {
            connectorProvider.prepareStatistics(Set.of(left, right));
        }
    }

    /** Invalidates cached statistics for the given table. */
    public void invalidate(QualifiedTableName tableName) {
        if (tableName != null && store.remove(tableName) != null) {
            LOG.info("event=statistics_invalidated table={}", tableName);
        }
    }

    // --- private helpers ---

    private org.pangrova.planner.api.statistics.ColumnStatistics convertFromSpi(
            ColumnStatistics spiCol, long tableRowCount) {
        ColumnStatistics evidence = spiCol == null ? ColumnStatistics.UNKNOWN : spiCol;
        return new org.pangrova.planner.api.statistics.ColumnStatistics(
                tableRowCount,
                evidence.nullCount(),
                evidence.min(), evidence.max(),
                evidence.ndv(),
                evidence.avgSize(),
                Optional.empty(),
                spiCol == null ? 0.5 : 1.0);
    }

    /**
     * Fallback column statistics when no connector provider is available.
     * Returns placeholder data (rowCount=1000, uniform distribution) with
     * confidence 0.5 — below the CBO threshold ({@code StatisticsBasedCostEstimator
     * .MIN_CONFIDENCE_THRESHOLD = 0.6}), so the cost estimator will ignore these
     * placeholders and use conservative defaults instead.
     *
     * @param columnName the column to collect statistics for
     * @return placeholder statistics with low confidence (0.5)
     */
    private org.pangrova.planner.api.statistics.ColumnStatistics collectFullColumn(String columnName) {
        long rowCount = 1000L;
        return new org.pangrova.planner.api.statistics.ColumnStatistics(
                rowCount, 0, 0L, rowCount, rowCount, 8.0, Optional.empty(), 0.5);
    }

    private double adaptiveSampleRate(long affectedRows) {
        if (affectedRows <= 1000) return 1.0;
        if (affectedRows <= 100_000) return 0.1;
        return 0.01;
    }

    private Map<String, org.pangrova.planner.api.statistics.ColumnStatistics> mergeIncremental(
            Map<String, org.pangrova.planner.api.statistics.ColumnStatistics> existing, double sampleRate) {
        Map<String, org.pangrova.planner.api.statistics.ColumnStatistics> merged = new HashMap<>();
        for (var entry : existing.entrySet()) {
            org.pangrova.planner.api.statistics.ColumnStatistics old = entry.getValue();
            double newConfidence = Math.max(0.1, old.confidenceScore() * sampleRate);
            merged.put(entry.getKey(), new org.pangrova.planner.api.statistics.ColumnStatistics(
                    old.rowCount(), old.nullCount(), old.min(), old.max(),
                    old.ndv(), old.avgSize(), old.histogram(),
                    Math.max(0.1, Math.min(1.0, newConfidence))));
        }
        return merged;
    }

    /**
     * Bridge interface for retrieving connector-level table statistics.
     * Implemented by the metadata layer to provide real statistics from Iceberg/Paimon.
     */
    @FunctionalInterface
    public interface ConnectorStatisticsProvider {
        org.pangrova.connector.api.metadata.table.TableStatistics statistics(QualifiedTableName tableName);

        default void prepareStatistics(Set<QualifiedTableName> tables) { }
    }
}
