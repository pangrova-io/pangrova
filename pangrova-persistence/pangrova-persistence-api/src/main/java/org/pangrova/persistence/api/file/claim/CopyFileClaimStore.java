package org.pangrova.persistence.api.file.claim;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/** Durable, cross-query exclusion for one exact source object version and target table UUID. */
@Experimental
public interface CopyFileClaimStore {

    enum ClaimResult {
        /** This execution durably acquired the file before dispatching any write. */
        ACQUIRED,
        /** The identical execution previously acquired the same immutable evidence. */
        SAME_EXECUTION,
        /** Another execution owns this file; its catalog outcome is not yet proven. */
        BLOCKED
    }

    /**
     * Atomically records a file before write dispatch. A pending claim is never
     * removed or recycled merely because its owner failed or timed out.
     * A conflicting byte digest, profile revision, or immutable identity fails closed.
     */
    ClaimResult claim(CopyFileClaim claim);

    /** Reads the durable admission evidence without implying a catalog commit. */
    Optional<CopyFileClaim> get(String claimKey, long loadGeneration);
}
