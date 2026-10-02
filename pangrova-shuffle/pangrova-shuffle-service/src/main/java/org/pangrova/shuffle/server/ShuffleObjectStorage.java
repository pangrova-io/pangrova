package org.pangrova.shuffle.server;

import java.util.List;

/** Shared immutable object storage for accepted canonical shuffle batches. */
interface ShuffleObjectStorage extends AutoCloseable {

    void putImmutable(String key, byte[] data, byte[] sha256);

    void verifyStored(String key, long sizeBytes, byte[] sha256);

    byte[] get(String key);

    void delete(List<String> keys);

    String deletePrefixPage(String prefix, String after, int limit);

    @Override
    void close();
}
