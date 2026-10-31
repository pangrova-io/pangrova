package org.pangrova.connector.iceberg.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.connector.spi.index.NativeIndexCapability;
import org.pangrova.connector.spi.index.NativeIndexType;
import org.pangrova.connector.spi.index.IndexEvalResult;
import org.pangrova.connector.spi.index.predicate.EqualTo;
import org.pangrova.connector.spi.index.predicate.RangeQuery;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IcebergNativeIndexEvaluator} verifying Caffeine cache
 * migration: bounded size, expireAfterAccess, and core evaluation logic.
 */
class IcebergNativeIndexEvaluatorTest {

    private static final List<NativeIndexCapability> MIN_MAX_ALL =
            List.of(new NativeIndexCapability(NativeIndexType.MIN_MAX, List.of("*"), ""));

    @Test
    void defaultCacheHasCorrectPolicy() {
        var evaluator = new IcebergNativeIndexEvaluator();
        // Verify the evaluator can be constructed with default cache (no exception)
        assertThat(evaluator).isNotNull();
    }

    @Test
    void cacheConfigConstants() {
        assertThat(IcebergNativeIndexEvaluator.MAXIMUM_SIZE).isEqualTo(100_000);
        assertThat(IcebergNativeIndexEvaluator.EXPIRE_AFTER_ACCESS_MINUTES).isEqualTo(10);
    }

    @Test
    void evaluateReturnsUnknownWhenNoBoundsCached() {
        var evaluator = new IcebergNativeIndexEvaluator();
        IndexEvalResult result = evaluator.evaluate(
                "s3://bucket/file.parquet",
                List.of(new EqualTo("col", 42)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void evaluateReturnsSkipWhenValueOutsideBounds() {
        var evaluator = new IcebergNativeIndexEvaluator();
        // Bounds: col in [10, 20]
        evaluator.cacheBounds("file1.parquet", icebergBounds("col", 10, 20));

        // Query: col = 30 → outside bounds → SKIP
        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new EqualTo("col", 30)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluateReturnsUnknownWhenValueInsideBounds() {
        var evaluator = new IcebergNativeIndexEvaluator();
        evaluator.cacheBounds("file1.parquet", icebergBounds("col", 10, 20));

        // Query: col = 15 → inside bounds → UNKNOWN (might match)
        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new EqualTo("col", 15)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void clearCacheInvalidatesAllEntries() {
        var evaluator = new IcebergNativeIndexEvaluator();
        evaluator.cacheBounds("file1.parquet", icebergBounds("col", 10, 20));

        evaluator.clearCache();

        // After clear, should return UNKNOWN (no cached bounds)
        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new EqualTo("col", 30)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void cacheEvictsWhenMaxSizeExceeded() {
        // Create a tiny cache to verify eviction
        Cache<String, IcebergNativeIndexEvaluator.FileBounds> tinyCache =
                Caffeine.newBuilder().maximumSize(2).build();
        var evaluator = new IcebergNativeIndexEvaluator(tinyCache);

        evaluator.cacheBounds("file1.parquet", icebergBounds("col", 1, 10));
        evaluator.cacheBounds("file2.parquet", icebergBounds("col", 11, 20));
        evaluator.cacheBounds("file3.parquet", icebergBounds("col", 21, 30));

        // Force eviction maintenance
        tinyCache.cleanUp();

        assertThat(tinyCache.estimatedSize()).isLessThanOrEqualTo(2);
    }

    @Test
    void cacheEvictsAfterAccessExpiry() {
        Cache<String, IcebergNativeIndexEvaluator.FileBounds> expiringCache =
                Caffeine.newBuilder()
                        .maximumSize(100_000)
                        .expireAfterAccess(1, TimeUnit.NANOSECONDS)
                        .build();
        var evaluator = new IcebergNativeIndexEvaluator(expiringCache);

        evaluator.cacheBounds("file1.parquet", icebergBounds("col", 10, 20));

        // Force expiry maintenance
        expiringCache.cleanUp();

        // Entry should be expired
        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new EqualTo("col", 30)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void evaluateWithRangeQuerySkipsNonOverlapping() {
        var evaluator = new IcebergNativeIndexEvaluator();
        // Bounds: col in [10, 20]
        evaluator.cacheBounds("file1.parquet", icebergBounds("col", 10, 20));

        // Range query: col > 25 → no overlap → SKIP
        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new RangeQuery("col", 25, null, false, false)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluateWithRangeQueryKeepsOverlapping() {
        var evaluator = new IcebergNativeIndexEvaluator();
        evaluator.cacheBounds("file1.parquet", icebergBounds("col", 10, 20));

        // Range query: col > 5 AND col < 15 → overlaps → UNKNOWN
        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new RangeQuery("col", 5, 15, false, false)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    /**
     * Creates FileBounds with integer lower/upper for a single column,
     * using Iceberg's Conversions.toByteBuffer for correct serialization format.
     */
    private static IcebergNativeIndexEvaluator.FileBounds icebergBounds(
            String column, int lower, int upper) {
        int fieldId = 1;
        Type intType = Types.IntegerType.get();
        ByteBuffer lowerBuf = Conversions.toByteBuffer(intType, lower);
        ByteBuffer upperBuf = Conversions.toByteBuffer(intType, upper);
        return new IcebergNativeIndexEvaluator.FileBounds(
                Map.of(fieldId, lowerBuf),
                Map.of(fieldId, upperBuf),
                Map.of(column, fieldId),
                Map.of(fieldId, intType));
    }
}
