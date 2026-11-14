package org.pangrova.planner.cost;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.planner.statistics.CardinalityFeedbackStore;
import org.pangrova.planner.api.statistics.ColumnStatistics;
import org.pangrova.planner.statistics.StatisticsCollector;
import org.pangrova.planner.statistics.PlannerTableStatistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Bridges {@link StatisticsCollector} data into Calcite CBO cost estimation.
 * Provides row count, selectivity, and confidence-weighted cardinality estimates.
 *
 * <p>Optionally integrates index-derived statistics via {@link IndexStatisticsProvider}
 * for more accurate cardinality estimates on indexed tables (R11.4).</p>
 *
 * <p>Validates: Requirements 2.5, 2.6, 2.7, 11.4</p>
 */
public class StatisticsBasedCostEstimator {

    private static final Logger LOG = LoggerFactory.getLogger(StatisticsBasedCostEstimator.class);

    static final double DEFAULT_ROW_COUNT = 1_000_000.0;
    static final double DEFAULT_SELECTIVITY = 0.5;

    /**
     * Minimum confidence threshold for trusting statistics in cost estimation.
     * Statistics with confidence below this threshold are treated as unreliable
     * and the estimator falls back to conservative defaults.
     */
    static final double MIN_CONFIDENCE_THRESHOLD = 0.6;

    private final StatisticsCollector collector;
    private final IndexStatisticsProvider indexStatisticsProvider;
    private final IndexCostFactor indexCostFactor;
    private final CardinalityFeedbackStore feedbackStore;

    public StatisticsBasedCostEstimator(StatisticsCollector collector) {
        this(collector, null, null, null);
    }

    public StatisticsBasedCostEstimator(StatisticsCollector collector,
                                        IndexStatisticsProvider indexStatisticsProvider) {
        this(collector, indexStatisticsProvider, null, null);
    }

    public StatisticsBasedCostEstimator(StatisticsCollector collector,
                                        IndexStatisticsProvider indexStatisticsProvider,
                                        IndexCostFactor indexCostFactor) {
        this(collector, indexStatisticsProvider, indexCostFactor, null);
    }

    /**
     * Creates a cost estimator with cardinality feedback support.
     *
     * @param collector table/column statistics source
     * @param indexStatisticsProvider index statistics provider
     * @param indexCostFactor cost adjustment factor for index scans
     * @param feedbackStore runtime cardinality feedback store, may be null
     */
    public StatisticsBasedCostEstimator(StatisticsCollector collector,
                                        IndexStatisticsProvider indexStatisticsProvider,
                                        IndexCostFactor indexCostFactor,
                                        CardinalityFeedbackStore feedbackStore) {
        this.collector = collector;
        this.indexStatisticsProvider = indexStatisticsProvider;
        this.indexCostFactor = indexCostFactor;
        this.feedbackStore = feedbackStore;
    }

    /** Estimate row count from statistics, or return default.
     * Checks index-derived statistics first (higher confidence from actual data),
     * then falls back to connector statistics. Ignores statistics with confidence
     * below threshold to avoid CBO decisions based on unreliable placeholder data. */
    public double estimateRowCount(QualifiedTableName tableName) {
        Optional<Double> indexRowCount = indexDerivedRowCount(tableName);
        if (indexRowCount.isPresent()) {
            LOG.debug("event=index_stats_used table={} rowCount={}", tableName, indexRowCount.get());
            return applyCorrectionFactor(tableName, indexRowCount.get());
        }
        double raw = firstConfidentColumnRowCount(tableName).orElse(DEFAULT_ROW_COUNT);
        return applyCorrectionFactor(tableName, raw);
    }

    private double applyCorrectionFactor(QualifiedTableName tableName, double rawEstimate) {
        if (feedbackStore == null) {
            return rawEstimate;
        }
        return feedbackStore.correctedEstimate(tableName.canonicalKey(), rawEstimate);
    }

    /**
     * Estimate selectivity for a predicate on a column.
     * Prefers index-derived NDV when available, falls back to connector statistics.
     */
    public double estimateSelectivity(QualifiedTableName tableName, String column) {
        Optional<ColumnStatistics> indexCol = indexColumnStats(tableName, column);
        if (indexCol.isPresent()) {
            ColumnStatistics cs = indexCol.get();
            if (cs.ndv() > 0) {
                LOG.debug("event=index_selectivity_used table={} column={} ndv={}",
                        tableName, column, cs.ndv());
                return 1.0 / cs.ndv();
            }
        }
        return columnStats(tableName, column)
                .filter(cs -> cs.confidenceScore() >= MIN_CONFIDENCE_THRESHOLD)
                .filter(cs -> cs.ndv() > 0)
                .map(cs -> 1.0 / cs.ndv())
                .orElse(DEFAULT_SELECTIVITY);
    }

    /**
     * Histogram-based equality selectivity: fraction of rows matching a specific value.
     * Finds the bucket containing the value and returns bucket.rowCount / totalRows.
     * Falls back to 1/NDV when no histogram is available.
     */
    public double histogramEqualitySelectivity(QualifiedTableName tableName, String column, Comparable<?> value) {
        Optional<org.pangrova.planner.api.statistics.Histogram> hist = getHistogram(tableName, column);
        if (hist.isEmpty() || value == null) {
            return estimateSelectivity(tableName, column);
        }
        org.pangrova.planner.api.statistics.Histogram histogram = hist.get();
        long totalRows = histogram.totalRows();
        if (totalRows <= 0) {
            return estimateSelectivity(tableName, column);
        }
        for (org.pangrova.planner.api.statistics.HistogramBucket bucket : histogram.buckets()) {
            if (valueInBucket(value, bucket)) {
                return bucket.distinctCount() > 0
                        ? (double) bucket.rowCount() / totalRows / bucket.distinctCount()
                        : (double) bucket.rowCount() / totalRows;
            }
        }
        return estimateSelectivity(tableName, column);
    }

    /**
     * Histogram-based range selectivity: fraction of rows in [lower, upper].
     * Sums matching buckets with linear interpolation at boundaries.
     * Falls back to default selectivity when no histogram is available.
     */
    public double histogramRangeSelectivity(QualifiedTableName tableName, String column,
                                            Comparable<?> lower, Comparable<?> upper) {
        Optional<org.pangrova.planner.api.statistics.Histogram> hist = getHistogram(tableName, column);
        if (hist.isEmpty()) {
            return DEFAULT_SELECTIVITY;
        }
        org.pangrova.planner.api.statistics.Histogram histogram = hist.get();
        long totalRows = histogram.totalRows();
        if (totalRows <= 0) {
            return DEFAULT_SELECTIVITY;
        }
        long matchingRows = 0;
        for (org.pangrova.planner.api.statistics.HistogramBucket bucket : histogram.buckets()) {
            double overlap = bucketRangeOverlap(bucket, lower, upper);
            matchingRows += (long) (bucket.rowCount() * overlap);
        }
        return Math.max(0.0, Math.min(1.0, (double) matchingRows / totalRows));
    }

    /**
     * Histogram-based IN list selectivity: sum of per-value equality selectivities.
     * Capped at 1.0.
     */
    public double histogramInListSelectivity(QualifiedTableName tableName, String column,
                                             java.util.List<Comparable<?>> values) {
        if (values == null || values.isEmpty()) {
            return DEFAULT_SELECTIVITY;
        }
        double total = 0.0;
        for (Comparable<?> value : values) {
            total += histogramEqualitySelectivity(tableName, column, value);
        }
        return Math.min(1.0, total);
    }

    private Optional<org.pangrova.planner.api.statistics.Histogram> getHistogram(QualifiedTableName tableName,
            String column) {
        return columnStats(tableName, column)
                .filter(cs -> cs.confidenceScore() >= MIN_CONFIDENCE_THRESHOLD)
                .flatMap(ColumnStatistics::histogram);
    }

    @SuppressWarnings("unchecked")
    private static boolean valueInBucket(Comparable<?> value,
                                         org.pangrova.planner.api.statistics.HistogramBucket bucket) {
        if (value instanceof Number numVal) {
            // Fast path for numeric types
            try {
                double v = numVal.doubleValue();
                double lo = ((Number) bucket.lowerBound()).doubleValue();
                double hi = ((Number) bucket.upperBound()).doubleValue();
                return v >= lo && v < hi;
            } catch (ClassCastException e) {
                return false;
            }
        }
        // Comparable-aware path: use natural ordering for String, Date, etc.
        if (value != null && bucket.lowerBound() != null && bucket.upperBound() != null
                && value.getClass().isInstance(bucket.lowerBound())) {
            @SuppressWarnings("rawtypes")
            Comparable lo = bucket.lowerBound();
            @SuppressWarnings("rawtypes")
            Comparable hi = bucket.upperBound();
            return ((Comparable) value).compareTo(lo) >= 0
                    && ((Comparable) value).compareTo(hi) < 0;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static double bucketRangeOverlap(org.pangrova.planner.api.statistics.HistogramBucket bucket,
                                              Comparable<?> lower, Comparable<?> upper) {
        try {
            double bLo = ((Number) bucket.lowerBound()).doubleValue();
            double bHi = ((Number) bucket.upperBound()).doubleValue();
            double rLo = lower != null ? ((Number) lower).doubleValue() : bLo;
            double rHi = upper != null ? ((Number) upper).doubleValue() : bHi;
            double overlapLo = Math.max(bLo, rLo);
            double overlapHi = Math.min(bHi, rHi);
            if (overlapLo >= overlapHi) {
                return 0.0;
            }
            double bucketWidth = bHi - bLo;
            return bucketWidth > 0 ? (overlapHi - overlapLo) / bucketWidth : 0.0;
        } catch (ClassCastException e) {
            // Non-numeric Comparable: use compareTo for containment check.
            // Without numeric distance we assume uniform 50% overlap when
            // the range intersects the bucket.
            Object bLoObj = bucket.lowerBound();
            Object bHiObj = bucket.upperBound();
            if (bLoObj instanceof Comparable && bHiObj instanceof Comparable) {
                @SuppressWarnings("rawtypes")
                Comparable bLo = (Comparable) bLoObj;
                @SuppressWarnings("rawtypes")
                Comparable bHi = (Comparable) bHiObj;
                @SuppressWarnings("rawtypes")
                Comparable lo = lower != null ? lower : bLo;
                @SuppressWarnings("rawtypes")
                Comparable hi = upper != null ? upper : bHi;
                boolean intersects = lo.compareTo(bHi) < 0 && hi.compareTo(bLo) > 0;
                return intersects ? 0.5 : 0.0;
            }
            return 0.0;
        }
    }

    /**
     * Confidence-weighted cardinality: rowCount × confidenceScore.
     * Falls back to default row count with minimum confidence when no stats.
     */
    public double confidenceWeightedCardinality(QualifiedTableName tableName) {
        return collector.getStatistics(tableName)
                .flatMap(this::weightedCardinality)
                .orElse(DEFAULT_ROW_COUNT * 0.1);
    }

    /**
     * Estimates scan cost for a table with optional index cost reduction (R11.2).
     * When an index exists on a filter column, the scan cost is reduced by the
     * index's historical skip ratio.
     *
     * @param tableName     fully qualified table name
     * @param filterColumns columns used in WHERE predicates
     * @return estimated scan cost (row count × index cost multiplier)
     */
    public double estimateScanCost(QualifiedTableName tableName, java.util.Set<String> filterColumns) {
        double rowCount = estimateRowCount(tableName);
        if (indexCostFactor == null) {
            return rowCount;
        }
        double multiplier = indexCostFactor.computeCostMultiplier(tableName, filterColumns);
        double cost = rowCount * multiplier;
        if (multiplier < 1.0) {
            LOG.info("event=index_cost_reduction table={} rowCount={} multiplier={} cost={}",
                    tableName, rowCount, multiplier, cost);
        }
        return cost;
    }

    private Optional<Double> weightedCardinality(PlannerTableStatistics stats) {
        return stats.columns().values().stream()
                .map(cs -> cs.rowCount() * cs.confidenceScore())
                .max(Double::compareTo);
    }

    private Optional<Double> firstConfidentColumnRowCount(QualifiedTableName tableName) {
        return collector.getStatistics(tableName)
                .flatMap(ts -> ts.columns().values().stream()
                        .filter(cs -> cs.confidenceScore() >= MIN_CONFIDENCE_THRESHOLD)
                        .mapToDouble(ColumnStatistics::rowCount)
                        .max()
                        .stream().boxed().findFirst());
    }

    private Optional<ColumnStatistics> columnStats(QualifiedTableName tableName, String column) {
        return collector.getStatistics(tableName)
                .map(ts -> ts.columns().get(column));
    }

    private Optional<Double> indexDerivedRowCount(QualifiedTableName tableName) {
        if (indexStatisticsProvider == null) {
            return Optional.empty();
        }
        try {
            Map<String, ColumnStatistics> indexStats =
                    indexStatisticsProvider.getIndexDerivedStatistics(tableName);
            return indexStats.values().stream()
                    .filter(cs -> cs.confidenceScore() >= MIN_CONFIDENCE_THRESHOLD)
                    .mapToDouble(ColumnStatistics::rowCount)
                    .max()
                    .stream().boxed().findFirst();
        } catch (Exception e) {
            LOG.debug("event=index_stats_failed table={} error={}", tableName, e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<ColumnStatistics> indexColumnStats(QualifiedTableName tableName, String column) {
        if (indexStatisticsProvider == null) {
            return Optional.empty();
        }
        try {
            Map<String, ColumnStatistics> indexStats =
                    indexStatisticsProvider.getIndexDerivedStatistics(tableName);
            return Optional.ofNullable(indexStats.get(column));
        } catch (Exception e) {
            LOG.debug("event=index_column_stats_failed table={} column={} error={}",
                    tableName, column, e.getMessage());
            return Optional.empty();
        }
    }
}
