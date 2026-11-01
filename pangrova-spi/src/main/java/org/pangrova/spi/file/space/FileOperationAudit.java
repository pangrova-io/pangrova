package org.pangrova.spi.file.space;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Records authorized file operations and signed-URL issuance.
 *
 * <p>Implementations must never receive storage credentials, signed URLs, or
 * object bytes: only the operation metadata is durable.</p>
 */
@Experimental
public interface FileOperationAudit {

    /** Records the operation performed on {@code path} through the named Worker. */
    void fileOperation(String principal, String tenantId, FileSpace space, String path,
            String operation, String workerEndpoint);

    /**
     * Records one signed-URL issuance.
     *
     * @param mode          grant mode, for example {@code presigned-read} or {@code stage}
     * @param ttlSeconds    granted lifetime, or zero when the grant is not time-bounded
     * @param workerEndpoint Worker that will honour the grant
     */
    void urlIssued(String principal, String tenantId, FileSpace space, String path,
            String mode, long ttlSeconds, String workerEndpoint);
}
