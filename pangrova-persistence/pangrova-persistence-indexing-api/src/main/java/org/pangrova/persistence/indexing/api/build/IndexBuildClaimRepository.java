package org.pangrova.persistence.indexing.api.build;

import java.time.Instant;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Atomic, thread-safe ownership operations for one index generation's scalar build.
 *
 * <p>The owner is a fresh, never-reused attempt ID, not a node or coordinator ID.
 * Acquisition is exclusive even for a repeated owner. Release and heartbeat return false
 * when the exact owner no longer holds the claim; they must never affect a replacement.
 * Backend failures propagate instead of being reported as contention or success.
 * Callers must stop the attempt after a rejected or failed heartbeat.</p>
 *
 * <p>This claim guards build work, not generation publication or object deletion.
 * Expiration requires an external recovery protocol that fences mappings, publication,
 * and files before admitting replacement work. It is not an automatic lease timeout.
 * Implementations borrow backend resources; this port does not close them.</p>
 */
@Experimental
public interface IndexBuildClaimRepository {
    /** Inserts a new claim, returning false if this index and generation are already claimed. */
    boolean tryClaim(String indexId, int generation, String ownerId, Instant claimedAt);

    /** Deletes only the exact attempt's claim; repeated release returns false. */
    boolean release(String indexId, int generation, String ownerId);

    /** Renews only the exact attempt, without moving its heartbeat backwards. */
    boolean heartbeat(String indexId, int generation, String ownerId, Instant heartbeatAt);

    /** Removes claims strictly older than the cutoff; the caller must fence abandoned work. */
    int expireBefore(Instant cutoff);
}
