package org.pangrova.shuffle.server;

import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

/** Shared immutable object storage for accepted canonical shuffle batches. */
interface ShuffleObjectStorage extends AutoCloseable {

    void putImmutable(String key, Supplier<InputStream> data, long sizeBytes, byte[] sha256);

    void verifyStored(String key, long sizeBytes, byte[] sha256);

    byte[] get(String key);

    void delete(List<String> keys);

    String deletePrefixPage(String prefix, String after, int limit);

    @Override
    void close();
}
