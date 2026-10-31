package org.pangrova.connector.paimon.index;

import org.pangrova.connector.spi.index.NativeIndexCapability;
import org.pangrova.connector.spi.index.NativeIndexEvaluator;
import org.pangrova.connector.spi.index.NativeIndexType;
import org.pangrova.connector.spi.index.IndexEvalResult;
import org.pangrova.connector.spi.index.predicate.EqualTo;
import org.pangrova.connector.spi.index.predicate.IndexPredicate;
import org.pangrova.connector.spi.index.predicate.RangeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Evaluates predicates against Paimon file-level statistics (min/max from manifest)
 * for L1 native index filtering.
 *
 * <p>Thread-safe. Bounds are cached externally via {@link #cacheBounds(String, FileBounds)}
 * during split enumeration when manifest statistics are already available.
 *
 * <p>The cache is bounded by {@code maximumSize(100_000)} and entries expire after
 * {@code 10 minutes} of no access, preventing unbounded memory growth.
 */
public final class PaimonNativeIndexEvaluator implements NativeIndexEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonNativeIndexEvaluator.class);
    static final long MAXIMUM_SIZE = 100_000;
    static final long EXPIRE_AFTER_ACCESS_MINUTES = 10;

    private final Cache<String, FileBounds> boundsCache;

    public PaimonNativeIndexEvaluator() {
        this(buildDefaultCache());
    }

    PaimonNativeIndexEvaluator(Cache<String, FileBounds> cache) {
        this.boundsCache = cache;
    }

    private static Cache<String, FileBounds> buildDefaultCache() {
        return Caffeine.newBuilder()
                .maximumSize(MAXIMUM_SIZE)
                .expireAfterAccess(EXPIRE_AFTER_ACCESS_MINUTES, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public IndexEvalResult evaluate(
            String dataFilePath,
            List<IndexPredicate> predicates,
            List<NativeIndexCapability> capabilities) {
        try {
            FileBounds bounds = boundsCache.getIfPresent(dataFilePath);
            if (bounds == null) {
                return IndexEvalResult.UNKNOWN;
            }
            for (IndexPredicate predicate : predicates) {
                if (canEvaluateWithMinMax(predicate, capabilities)
                        && !boundsOverlap(bounds, predicate)) {
                    return IndexEvalResult.SKIP;
                }
            }
            return IndexEvalResult.UNKNOWN;
        } catch (Exception e) {
            LOG.debug("action=native_eval_failed file={} error={}", dataFilePath, e.getMessage());
            return IndexEvalResult.UNKNOWN;
        }
    }

    /**
     * Caches file bounds extracted from Paimon manifest statistics.
     *
     * @param dataFilePath the data file path
     * @param bounds       the min/max bounds for this file
     */
    public void cacheBounds(String dataFilePath, FileBounds bounds) {
        Objects.requireNonNull(dataFilePath, "dataFilePath must not be null");
        Objects.requireNonNull(bounds, "bounds must not be null");
        boundsCache.put(dataFilePath, bounds);
    }

    /** Clears the bounds cache. */
    public void clearCache() {
        boundsCache.invalidateAll();
    }

    private static boolean canEvaluateWithMinMax(
            IndexPredicate predicate, List<NativeIndexCapability> capabilities) {
        for (NativeIndexCapability cap : capabilities) {
            if (cap.indexType() == NativeIndexType.MIN_MAX) {
                if (cap.columnNames().contains("*")
                        || cap.coversColumn(predicate.column())) {
                    return (predicate instanceof EqualTo)
                            || (predicate instanceof RangeQuery);
                }
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static boolean boundsOverlap(FileBounds bounds, IndexPredicate predicate) {
        String column = predicate.column();
        Comparable<Object> lower = (Comparable<Object>) bounds.lowerBound(column);
        Comparable<Object> upper = (Comparable<Object>) bounds.upperBound(column);
        if (lower == null || upper == null) {
            return true; // no bounds, conservatively keep
        }

        if (predicate instanceof EqualTo eq) {
            Comparable<Object> value = (Comparable<Object>) eq.value();
            return lower.compareTo(value) <= 0 && upper.compareTo(value) >= 0;
        }

        if (predicate instanceof RangeQuery range) {
            return rangeOverlaps(lower, upper, range);
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean rangeOverlaps(
            Comparable<Object> fileLower, Comparable<Object> fileUpper, RangeQuery range) {
        return !fileMaxBelowQueryMin(fileUpper, range)
                && !fileMinAboveQueryMax(fileLower, range);
    }

    @SuppressWarnings("unchecked")
    private static boolean fileMaxBelowQueryMin(Comparable<Object> fileUpper, RangeQuery range) {
        if (range.lower() == null) {
            return false;
        }
        Comparable<Object> queryLower = (Comparable<Object>) range.lower();
        int cmp = fileUpper.compareTo(queryLower);
        return range.lowerInclusive() ? cmp < 0 : cmp <= 0;
    }

    @SuppressWarnings("unchecked")
    private static boolean fileMinAboveQueryMax(Comparable<Object> fileLower, RangeQuery range) {
        if (range.upper() == null) {
            return false;
        }
        Comparable<Object> queryUpper = (Comparable<Object>) range.upper();
        int cmp = fileLower.compareTo(queryUpper);
        return range.upperInclusive() ? cmp > 0 : cmp >= 0;
    }

    /**
     * Holds min/max bounds for a single Paimon data file.
     * Column bounds are stored as domain-level Comparable values.
     */
    public static final class FileBounds {

        private final Map<String, Comparable<?>> lowerBounds;
        private final Map<String, Comparable<?>> upperBounds;

        public FileBounds(
                Map<String, Comparable<?>> lowerBounds,
                Map<String, Comparable<?>> upperBounds) {
            this.lowerBounds = Map.copyOf(lowerBounds);
            this.upperBounds = Map.copyOf(upperBounds);
        }

        Comparable<?> lowerBound(String column) {
            return lowerBounds.get(column);
        }

        Comparable<?> upperBound(String column) {
            return upperBounds.get(column);
        }
    }
}
