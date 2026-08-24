package org.pangrova.connector.iceberg.index;

import org.pangrova.spi.connector.index.NativeIndexCapability;
import org.pangrova.spi.connector.index.NativeIndexEvaluator;
import org.pangrova.spi.connector.index.NativeIndexType;
import org.pangrova.spi.index.IndexEvalResult;
import org.pangrova.spi.index.predicate.EqualTo;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.predicate.RangeQuery;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Evaluates predicates against Iceberg manifest entry bounds (min/max statistics)
 * for L1 native index filtering.
 *
 * <p>Thread-safe. Uses a per-file bounds cache to avoid repeated manifest scans.
 * The cache is populated externally via {@link #cacheBounds(String, FileBounds)}
 * during split enumeration when manifest entries are already available.
 *
 * <p>The cache is bounded by {@code maximumSize(100_000)} and entries expire after
 * {@code 10 minutes} of no access, preventing unbounded memory growth.
 */
public final class IcebergNativeIndexEvaluator implements NativeIndexEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergNativeIndexEvaluator.class);
    static final long MAXIMUM_SIZE = 100_000;
    static final long EXPIRE_AFTER_ACCESS_MINUTES = 10;

    private final Cache<String, FileBounds> boundsCache;

    public IcebergNativeIndexEvaluator() {
        this(buildDefaultCache());
    }

    IcebergNativeIndexEvaluator(Cache<String, FileBounds> cache) {
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
     * Caches file bounds extracted from Iceberg manifest entries.
     * Called during split enumeration when manifest data is already loaded.
     *
     * @param dataFilePath the data file path
     * @param bounds       the min/max bounds for this file
     */
    public void cacheBounds(String dataFilePath, FileBounds bounds) {
        Objects.requireNonNull(dataFilePath, "dataFilePath must not be null");
        Objects.requireNonNull(bounds, "bounds must not be null");
        boundsCache.put(dataFilePath, bounds);
    }

    /**
     * Populates bounds cache from an Iceberg ContentFile's column statistics.
     *
     * <p>Uses the table schema to resolve field types so that ByteBuffer bounds
     * can be deserialized to typed Comparable values for correct min/max pruning.
     *
     * @param file      the Iceberg content file with statistics
     * @param fieldIds  mapping from column name to Iceberg field ID
     * @param schema    the Iceberg table schema for type resolution
     */
    public void cacheFromContentFile(ContentFile<?> file, Map<String, Integer> fieldIds,
                                     Schema schema) {
        Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
        Map<Integer, ByteBuffer> upperBounds = file.upperBounds();
        if (lowerBounds == null || upperBounds == null) {
            return;
        }
        Map<Integer, Type> fieldTypes = new HashMap<>();
        for (Integer fieldId : fieldIds.values()) {
            Types.NestedField field = schema.findField(fieldId);
            if (field != null) {
                fieldTypes.put(fieldId, field.type());
            }
        }
        boundsCache.put(file.path().toString(),
                new FileBounds(lowerBounds, upperBounds, fieldIds, fieldTypes));
    }

    /** Clears the bounds cache. */
    public void clearCache() {
        boundsCache.invalidateAll();
    }

    private static boolean canEvaluateWithMinMax(
            IndexPredicate predicate, List<NativeIndexCapability> capabilities) {
        for (NativeIndexCapability cap : capabilities) {
            if (cap.indexType() == NativeIndexType.MIN_MAX) {
                // "*" means all columns, or check specific column
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
            return true; // no bounds available, conservatively keep
        }

        if (predicate instanceof EqualTo eq) {
            Comparable<Object> value = (Comparable<Object>) eq.value();
            // file range [lower, upper] overlaps with point value iff lower <= value <= upper
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
     * Holds min/max bounds for a single data file, extracted from Iceberg manifest entries.
     *
     * <p>Stores raw ByteBuffer bounds keyed by Iceberg field ID, with a name-to-ID mapping
     * for column name resolution and a field-ID-to-Type mapping for typed deserialization.
     * Bounds are deserialized to typed Comparable values on access using
     * {@link Conversions#fromByteBuffer(Type, ByteBuffer)}.
     */
    public static final class FileBounds {

        private final Map<Integer, ByteBuffer> lowerBounds;
        private final Map<Integer, ByteBuffer> upperBounds;
        private final Map<String, Integer> fieldIds;
        private final Map<Integer, Type> fieldTypes;

        /**
         * Creates bounds from raw metadata-column maps, defensively copying all inputs.
         */
        public FileBounds(
                Map<Integer, ByteBuffer> lowerBounds,
                Map<Integer, ByteBuffer> upperBounds,
                Map<String, Integer> fieldIds,
                Map<Integer, Type> fieldTypes) {
            this.lowerBounds = Map.copyOf(lowerBounds);
            this.upperBounds = Map.copyOf(upperBounds);
            this.fieldIds = Map.copyOf(fieldIds);
            this.fieldTypes = Map.copyOf(fieldTypes);
        }

        /**
         * Returns the lower bound for the given column as a typed Comparable,
         * or null if unavailable or the type is unknown.
         */
        Comparable<?> lowerBound(String column) {
            return deserializeBound(column, lowerBounds);
        }

        /**
         * Returns the upper bound for the given column as a typed Comparable,
         * or null if unavailable or the type is unknown.
         */
        Comparable<?> upperBound(String column) {
            return deserializeBound(column, upperBounds);
        }

        private Comparable<?> deserializeBound(String column, Map<Integer, ByteBuffer> bounds) {
            Integer fieldId = fieldIds.get(column);
            if (fieldId == null) {
                return null;
            }
            ByteBuffer buf = bounds.get(fieldId);
            if (buf == null) {
                return null;
            }
            Type type = fieldTypes.get(fieldId);
            if (type == null) {
                return null;
            }
            Object value = Conversions.fromByteBuffer(type, buf.asReadOnlyBuffer());
            return value instanceof Comparable<?> c ? c : null;
        }
    }

}
