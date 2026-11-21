package org.pangrova.persistence.file.api.claim;

import org.pangrova.persistence.file.api.enums.CopyFileClaimResult;
import org.pangrova.common.api.annotation.Experimental;

import java.util.Optional;

/** Durable, cross-query exclusion for one exact source object version and target table UUID. */
@Experimental
public interface CopyFileClaimStore {

    /**
     * Atomically records a file before write dispatch. A pending claim is never
     * removed or recycled merely because its owner failed or timed out.
     * A conflicting byte digest, profile revision, or immutable identity fails closed.
     */
    CopyFileClaimResult claim(CopyFileClaim claim);

    /** Reads the durable admission evidence without implying a catalog commit. */
    Optional<CopyFileClaim> get(String claimKey, long loadGeneration);
}
