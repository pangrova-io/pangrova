package org.pangrova.persistence.catalog.api.invalidation;

import java.util.function.Consumer;

import org.pangrova.metadata.api.cache.InvalidationEntry.InvalidationType;

/**
 * Cross-coordinator invalidation-log participant: reads committed entries and accepts
 * result-cache invalidations. The owner retains polling lifecycle and resource closure.
 */
public interface InvalidationLogPoller {

    /** Registers the local result-cache invalidation callback. */
    void setResultCacheInvalidator(Consumer<String> invalidator);

    /** Writes one committed invalidation entry for other coordinators to read. */
    void writeInvalidation(String cacheKey, InvalidationType invalidationType);
}
