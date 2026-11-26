package org.pangrova.persistence.query.api.lifecycle;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;
import java.util.Set;

/** Durable responsibility to clean up remote Shuffle scopes of a terminal query. */
@Experimental
public interface ShuffleCleanupIntentStore {

    /** Records cleanup responsibility before a terminal query state is published. */
    void markPending(String queryId);

    /** Returns whether cleanup remains due, including after Coordinator restart. */
    boolean isPending(String queryId);

    /** Returns pending cleanup identities within the supplied recovery batch. */
    Set<String> pendingForQueries(Set<String> queryIds);

    /** Keyset page of pending cleanups whose execution anchor is owned by {@code ownerId}. */
    List<String> pendingForOwnerAfter(String ownerId, String afterQueryId, int limit);
}
