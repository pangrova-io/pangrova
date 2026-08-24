package org.pangrova.spi.cache;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CacheKey} SPI contract.
 * G-CACHE-08: distributed/detached cache layer SPI.
 */
class CacheKeyTest {

    @Test
    void constructorStoresFields() {
        CacheKey key = new CacheKey("s3://bucket/table/data/00001.parquet", 1024, 4096);
        assertThat(key.filePath()).isEqualTo("s3://bucket/table/data/00001.parquet");
        assertThat(key.offset()).isEqualTo(1024);
        assertThat(key.length()).isEqualTo(4096);
    }

    @Test
    void constructorRejectsNullFilePath() {
        assertThatThrownBy(() -> new CacheKey(null, 0, 100))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("filePath");
    }

    @Test
    void constructorRejectsNegativeOffset() {
        assertThatThrownBy(() -> new CacheKey("s3://bucket/file.parquet", -1, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset");
    }

    @Test
    void constructorRejectsZeroLength() {
        assertThatThrownBy(() -> new CacheKey("s3://bucket/file.parquet", 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length");
    }

    @Test
    void constructorRejectsNegativeLength() {
        assertThatThrownBy(() -> new CacheKey("s3://bucket/file.parquet", 0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length");
    }

    @Test
    void wholeFileFactory() {
        CacheKey key = CacheKey.wholeFile("s3://bucket/file.parquet");
        assertThat(key.filePath()).isEqualTo("s3://bucket/file.parquet");
        assertThat(key.offset()).isEqualTo(0);
        assertThat(key.length()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void toKeyStringFormat() {
        CacheKey key = new CacheKey("s3://bucket/file.parquet", 1024, 4096);
        assertThat(key.toKeyString()).isEqualTo("s3://bucket/file.parquet#1024+4096");
    }

    @Test
    void equalKeysAreEqual() {
        CacheKey a = new CacheKey("s3://bucket/file.parquet", 1024, 4096);
        CacheKey b = new CacheKey("s3://bucket/file.parquet", 1024, 4096);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void differentFilePathsAreNotEqual() {
        CacheKey a = new CacheKey("s3://bucket/a.parquet", 0, 100);
        CacheKey b = new CacheKey("s3://bucket/b.parquet", 0, 100);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void differentOffsetsAreNotEqual() {
        CacheKey a = new CacheKey("s3://bucket/file.parquet", 0, 100);
        CacheKey b = new CacheKey("s3://bucket/file.parquet", 1, 100);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void differentLengthsAreNotEqual() {
        CacheKey a = new CacheKey("s3://bucket/file.parquet", 0, 100);
        CacheKey b = new CacheKey("s3://bucket/file.parquet", 0, 200);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void zeroOffsetIsValid() {
        CacheKey key = new CacheKey("s3://bucket/file.parquet", 0, 100);
        assertThat(key.offset()).isEqualTo(0);
    }

    @Test
    void toStringContainsKeyString() {
        CacheKey key = new CacheKey("s3://bucket/file.parquet", 1024, 4096);
        assertThat(key.toString()).contains("s3://bucket/file.parquet#1024+4096");
    }
}
