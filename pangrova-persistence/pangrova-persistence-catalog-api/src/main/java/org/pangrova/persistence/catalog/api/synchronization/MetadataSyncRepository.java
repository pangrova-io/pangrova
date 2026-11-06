package org.pangrova.persistence.catalog.api.synchronization;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.common.api.annotation.Experimental;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Thread-safe owner of durable sync progress and run diagnostics. Connections remain provider-owned.
 * Progress is never derived from diagnostic completion times. Implementations must atomically
 * compare the current deployment and expected checkpoint, store the run, and advance progress.
 * Concurrent/stale progress is rejected; identical run-ID replay succeeds, differing replay fails.
 * Completion uncertainty must be reported explicitly and confirmed with findCompletion before replay.
 * A deployment replacement has independent progress. History may be read after deployment deletion.
 */
@Experimental
public interface MetadataSyncRepository {
    Optional<Instant> loadCheckpoint(CatalogDeploymentIdentity scope);
    /**
     * Persists an immutable intent before executing completion. Known execution failures after
     * acceptance raise MetadataSyncPendingException; unknown commit outcomes raise
     * MetadataSyncOutcomeUnknownException. Neither permits replacing the run ID.
     */
    void complete(MetadataSyncCompletion completion);

    /**
     * Resolves durable pending completion by its original ID/content before reading a new source
     * batch, including after process loss. New completion IDs cannot bypass unresolved work.
     * Obsolete deployment work is rejected under the current deployment fence, never replayed
     * against its replacement. Failure propagates; this is not a lease over an entire scan.
     */
    void recoverPending(CatalogDeploymentIdentity scope);
    Optional<MetadataSyncCompletion> findCompletion(UUID runId);
}
