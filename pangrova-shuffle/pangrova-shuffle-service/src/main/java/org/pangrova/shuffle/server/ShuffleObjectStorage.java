package org.pangrova.shuffle.server;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/** Shared immutable object storage for accepted canonical shuffle batches. */
interface ShuffleObjectStorage extends AutoCloseable {

    void putImmutable(String key, Supplier<InputStream> data, long sizeBytes, byte[] sha256);

    void putManifestNode(String key, byte[] bytes, byte[] sha256, UUID reservationId);

    Optional<ManifestWriteReceipt> manifestWriteReceipt(String key);

    void verifyStored(String key, long sizeBytes, byte[] sha256);

    byte[] get(String key, long sizeBytes);

    void delete(List<String> keys);

    ObjectPage listPrefixPage(String prefix, String after, int limit);

    record ObjectPage(List<String> keys, String next) {
        public ObjectPage {
            keys = List.copyOf(keys);
        }
    }

    record ManifestWriteReceipt(String objectKey, UUID reservationId) {}

    @Override
    void close();
}
