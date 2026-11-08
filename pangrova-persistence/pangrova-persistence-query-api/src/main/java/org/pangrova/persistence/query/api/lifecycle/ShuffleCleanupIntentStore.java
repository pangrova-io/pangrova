package org.pangrova.persistence.query.api.lifecycle;

import java.util.List;

/** Durable responsibility to clean up remote Shuffle scopes of a terminal query. */
public interface ShuffleCleanupIntentStore {

    /** Records cleanup responsibility before a terminal query state is published. */
    void markPending(String queryId);

    /** Returns whether cleanup remains due, including after Coordinator restart. */
    boolean isPending(String queryId);

    /** Keyset page of pending cleanups whose execution anchor is owned by {@code ownerId}. */
    List<String> pendingForOwnerAfter(String ownerId, String afterQueryId, int limit);
}
