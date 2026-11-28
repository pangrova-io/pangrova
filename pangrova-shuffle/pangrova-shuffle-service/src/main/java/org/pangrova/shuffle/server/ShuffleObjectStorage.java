package org.pangrova.shuffle.server;

import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

/** Shared immutable object storage for accepted canonical shuffle batches. */
interface ShuffleObjectStorage extends AutoCloseable {

    void putImmutable(String key, Supplier<InputStream> data, long sizeBytes, byte[] sha256);

    void verifyStored(String key, long sizeBytes, byte[] sha256);

    byte[] get(String key, long sizeBytes);

    void delete(List<String> keys);

    ObjectPage listPrefixPage(String prefix, String after, int limit);

    record ObjectPage(List<String> keys, String next) {
        public ObjectPage {
            keys = List.copyOf(keys);
        }
    }

    @Override
    void close();
}
