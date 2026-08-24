package org.pangrova.spi.cache;

import org.pangrova.spi.annotation.Experimental;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable wrapper for a cached data block, carrying the payload bytes
 * and metadata needed for cache management (creation time, source info).
 *
 * <p>The payload is an opaque byte array -- typically a raw columnar block
 * (e.g. Parquet page, ORC stripe) or a serialized Arrow record batch.
 * The distributed cache store treats it as opaque bytes; interpretation
 * is left to the reader.</p>
 *
 * <p>G-CACHE-08: distributed/detached cache layer SPI.</p>
 */
@Experimental(reason = "G-CACHE-08: distributed cache entry model")
public final class CacheEntry {

    private final byte[] data;
    private final Instant createdAt;
    private final String tableId;

    /**
     * Creates a cache entry.
     *
     * @param data      the cached block payload; must not be null or empty
     * @param createdAt when the entry was created (wall-clock)
     * @param tableId   logical table identifier for bulk invalidation
     *                  (e.g. {@code "catalog.schema.table"}); must not be null
     */
    public CacheEntry(byte[] data, Instant createdAt, String tableId) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("data must not be null or empty");
        }
        this.data = data.clone(); // defensive copy
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        this.tableId = Objects.requireNonNull(tableId, "tableId must not be null");
    }

    /**
     * Convenience factory that uses the current time as creation timestamp.
     */
    public static CacheEntry of(byte[] data, String tableId) {
        return new CacheEntry(data, Instant.now(), tableId);
    }

    /**
     * Returns a copy of the cached payload bytes.
     */
    public byte[] data() {
        return data.clone();
    }

    /**
     * Returns the raw payload length in bytes (without copying).
     */
    public int dataLength() {
        return data.length;
    }

    public Instant createdAt() {
        return createdAt;
    }

    /**
     * Logical table identifier for bulk invalidation. Format is
     * connector-defined but typically {@code "catalog.schema.table"}.
     */
    public String tableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "CacheEntry{tableId=" + tableId
                + ", size=" + data.length
                + ", createdAt=" + createdAt + "}";
    }
}
