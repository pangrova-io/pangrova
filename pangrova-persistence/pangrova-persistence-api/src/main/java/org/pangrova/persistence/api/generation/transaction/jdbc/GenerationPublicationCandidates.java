package org.pangrova.persistence.api.generation.transaction.jdbc;

import org.pangrova.spi.generation.job.GenerationJobKey;
import org.pangrova.common.api.annotation.Internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

/** Cross-domain read projection of Artifact publication leases and Query execution ownership. */
@Internal
public interface GenerationPublicationCandidates {
    /**
     * Selects ready jobs whose publication lease is absent/expired or whose persisted
     * execution owner is no longer live. Filter before ordering by updated time,
     * tenant and job, then limit; do not fetch a page and filter it in memory.
     * Uses only the supplied connection and does not lock execution anchors.
     */
    List<GenerationJobKey> listReady(Connection connection, int limit, Instant now) throws SQLException;

    /**
     * Tests the persisted publication owner's current backend identity and database-clock expiry.
     * The caller has already fenced its own execution and locked the exact job. This read
     * must not add an anchor lock after the job lock or manage the caller's transaction.
     */
    boolean hasLiveOwner(Connection connection, String tenantId, String jobId) throws SQLException;
}
