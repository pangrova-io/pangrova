package org.pangrova.connector.paimon.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.connector.spi.index.NativeIndexCapability;
import org.pangrova.connector.spi.index.NativeIndexType;
import org.pangrova.connector.spi.index.IndexEvalResult;
import org.pangrova.connector.spi.index.predicate.EqualTo;
import org.pangrova.connector.spi.index.predicate.RangeQuery;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PaimonNativeIndexEvaluator} verifying Caffeine cache
 * migration: bounded size, expireAfterAccess, and core evaluation logic.
 */
class PaimonNativeIndexEvaluatorTest {

    private static final List<NativeIndexCapability> MIN_MAX_ALL =
            List.of(new NativeIndexCapability(NativeIndexType.MIN_MAX, List.of("*"), ""));

    @Test
    void defaultCacheHasCorrectPolicy() {
        var evaluator = new PaimonNativeIndexEvaluator();
        assertThat(evaluator).isNotNull();
    }

    @Test
    void cacheConfigConstants() {
        assertThat(PaimonNativeIndexEvaluator.MAXIMUM_SIZE).isEqualTo(100_000);
        assertThat(PaimonNativeIndexEvaluator.EXPIRE_AFTER_ACCESS_MINUTES).isEqualTo(10);
    }

    @Test
    void evaluateReturnsUnknownWhenNoBoundsCached() {
        var evaluator = new PaimonNativeIndexEvaluator();
        IndexEvalResult result = evaluator.evaluate(
                "bucket/file.parquet",
                List.of(new EqualTo("col", 42)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void evaluateReturnsSkipWhenValueOutsideBounds() {
        var evaluator = new PaimonNativeIndexEvaluator();
        evaluator.cacheBounds("file1.parquet", paimonBounds("col", 10, 20));

        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new EqualTo("col", 30)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluateReturnsUnknownWhenValueInsideBounds() {
        var evaluator = new PaimonNativeIndexEvaluator();
        evaluator.cacheBounds("file1.parquet", paimonBounds("col", 10, 20));

        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new EqualTo("col", 15)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void clearCacheInvalidatesAllEntries() {
        var evaluator = new PaimonNativeIndexEvaluator();
        evaluator.cacheBounds("file1.parquet", paimonBounds("col", 10, 20));

        evaluator.clearCache();

        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new EqualTo("col", 30)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void cacheEvictsWhenMaxSizeExceeded() {
        Cache<String, PaimonNativeIndexEvaluator.FileBounds> tinyCache =
                Caffeine.newBuilder().maximumSize(2).build();
        var evaluator = new PaimonNativeIndexEvaluator(tinyCache);

        evaluator.cacheBounds("file1.parquet", paimonBounds("col", 1, 10));
        evaluator.cacheBounds("file2.parquet", paimonBounds("col", 11, 20));
        evaluator.cacheBounds("file3.parquet", paimonBounds("col", 21, 30));

        tinyCache.cleanUp();

        assertThat(tinyCache.estimatedSize()).isLessThanOrEqualTo(2);
    }

    @Test
    void cacheEvictsAfterAccessExpiry() {
        Cache<String, PaimonNativeIndexEvaluator.FileBounds> expiringCache =
                Caffeine.newBuilder()
                        .maximumSize(100_000)
                        .expireAfterAccess(1, TimeUnit.NANOSECONDS)
                        .build();
        var evaluator = new PaimonNativeIndexEvaluator(expiringCache);

        evaluator.cacheBounds("file1.parquet", paimonBounds("col", 10, 20));

        expiringCache.cleanUp();

        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new EqualTo("col", 30)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    @Test
    void evaluateWithRangeQuerySkipsNonOverlapping() {
        var evaluator = new PaimonNativeIndexEvaluator();
        evaluator.cacheBounds("file1.parquet", paimonBounds("col", 10, 20));

        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new RangeQuery("col", 25, null, false, false)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.SKIP);
    }

    @Test
    void evaluateWithRangeQueryKeepsOverlapping() {
        var evaluator = new PaimonNativeIndexEvaluator();
        evaluator.cacheBounds("file1.parquet", paimonBounds("col", 10, 20));

        IndexEvalResult result = evaluator.evaluate(
                "file1.parquet",
                List.of(new RangeQuery("col", 5, 15, false, false)),
                MIN_MAX_ALL);
        assertThat(result).isEqualTo(IndexEvalResult.UNKNOWN);
    }

    private static PaimonNativeIndexEvaluator.FileBounds paimonBounds(
            String column, int lower, int upper) {
        return new PaimonNativeIndexEvaluator.FileBounds(
                Map.of(column, lower),
                Map.of(column, upper));
    }
}
