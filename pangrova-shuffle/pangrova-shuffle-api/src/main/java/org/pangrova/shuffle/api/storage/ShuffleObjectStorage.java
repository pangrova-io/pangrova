package org.pangrova.shuffle.api.storage;

import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

import org.pangrova.spi.annotation.Experimental;

/**
 * Shared immutable object storage for accepted canonical shuffle batches (ADR-002 D4/D6).
 *
 * <p>This is the storage primitive behind the remote shuffle service's accepted-batch plane:
 * batches are written once under their content hash and must never be overwritten with different
 * bytes. Implementations are selected by a stable name ({@code "local"}, {@code "s3"}, {@code "oss"},
 * ...) through {@link ShuffleObjectStorageFactory} and {@code META-INF/services}, mirroring
 * {@link org.pangrova.shuffle.api.spool.SpoolStoreFactory}.
 */
@Experimental(reason = "ADR-002 D4/D6 accepted-batch storage SPI; evolving alongside storage backends")
public interface ShuffleObjectStorage extends AutoCloseable {

    void putImmutable(String key, Supplier<InputStream> data, long sizeBytes, byte[] sha256);

    void verifyStored(String key, long sizeBytes, byte[] sha256);

    byte[] get(String key);

    /**
     * Returns all stored keys under {@code keyPrefix} in lexicographic order, in the same
     * key space as {@link #get} and {@link #delete}. Used by spool readers to enumerate
     * committed segments.
     */
    List<String> listPrefix(String keyPrefix);

    void delete(List<String> keys);

    String deletePrefixPage(String prefix, String after, int limit);

    @Override
    void close();
}
