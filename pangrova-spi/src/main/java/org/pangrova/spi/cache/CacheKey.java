package org.pangrova.spi.cache;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;

/**
 * Immutable key for the distributed data cache. Identifies a cached block
 * or fragment by its source file path and byte range within that file.
 *
 * <p>The key space intentionally matches the native data cache key model
 * (file path + offset + length) so that local and distributed caches can
 * share a common addressing scheme without translation.</p>
 *
 * <p>G-CACHE-08: distributed/detached cache layer SPI.</p>
 */
@Experimental(reason = "G-CACHE-08: distributed cache key model")
public final class CacheKey {

    private final String filePath;
    private final long offset;
    private final int length;

    /**
     * Creates a cache key for the given file region.
     *
     * @param filePath full storage URI (e.g. {@code s3://bucket/table/data/00001.parquet})
     * @param offset   byte offset of the cached block within the file
     * @param length   byte length of the cached block
     * @throws NullPointerException     if filePath is null
     * @throws IllegalArgumentException if offset is negative or length is non-positive
     */
    public CacheKey(String filePath, long offset, int length) {
        Objects.requireNonNull(filePath, "filePath must not be null");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative, got: " + offset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive, got: " + length);
        }
        this.filePath = filePath;
        this.offset = offset;
        this.length = length;
    }

    /**
     * Convenience factory for a whole-file cache key (offset=0, length=Integer.MAX_VALUE).
     * Useful when caching metadata or small files that are always read entirely.
     *
     * @param filePath full storage URI
     * @return a cache key covering the entire file
     */
    public static CacheKey wholeFile(String filePath) {
        return new CacheKey(filePath, 0, Integer.MAX_VALUE);
    }

    public String filePath() {
        return filePath;
    }

    public long offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    /**
     * Returns a compact string representation suitable for use as an
     * external store key (e.g. Redis key or filesystem path component).
     */
    public String toKeyString() {
        return filePath + "#" + offset + "+" + length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CacheKey that)) return false;
        return offset == that.offset
                && length == that.length
                && filePath.equals(that.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, offset, length);
    }

    @Override
    public String toString() {
        return "CacheKey{" + toKeyString() + "}";
    }
}
