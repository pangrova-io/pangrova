package org.pangrova.cache.api;

import java.time.Duration;

/** Thread-safe expendable values. Each operation is atomic within the deployed store. */
public interface CacheStore extends AutoCloseable {
    CacheLookup read(CacheKey key);

    /** Stores only if neither generation has changed since the read. Values expire after ttl. */
    boolean putIfCurrent(CacheLookup observed, String value, Duration ttl);

    void invalidate(String group, String entry);

    void invalidateGroup(String group);

    /** Releases only this instance's resources. Other processes' values are retained. */
    @Override void close();
}
