package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.time.Instant;
import java.util.List;

/** Cross-coordinator descriptor invalidation outbox and cursor reads. */
@Experimental
public interface IndexCacheEvents {
    /** Inserts a cache invalidation event. */
    default void insertCacheEvent(String eventType, String indexId,
                                  String catalog, String schema, String table, Instant now) {
        throw new UnsupportedOperationException("Index cache invalidation events unavailable");
    }

    /** Polls cache events after the given event ID. */
    default List<CacheEvent> pollCacheEvents(long afterEventId, int limit) {
        throw new UnsupportedOperationException("Index cache invalidation events unavailable");
    }
}
