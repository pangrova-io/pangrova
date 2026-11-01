package org.pangrova.streaming.cdc;

/**
 * Enforces write-time security policies before a CDC pipeline writes data
 * to the target table.
 *
 * <p>Implementations typically delegate to the system's {@code AccessControl}
 * SPI to verify that the pipeline creator (identified by the
 * {@link PipelineSecurityContext}) still has INSERT privilege on the target
 * table. This ensures that revoked permissions are detected at write time,
 * not silently bypassed by long-running pipelines.</p>
 *
 * <p>The interface is intentionally narrow (single method) to keep the
 * streaming module decoupled from the full security SPI. The coordinator
 * wires the implementation via a lambda that calls
 * {@code accessControl.checkCanInsertIntoTable(...)}.</p>
 *
 * @see PipelineSecurityContext
 * @see CdcPipelineLoop
 */
@FunctionalInterface
public interface WritePolicyEnforcer {

    /**
     * Validates that the pipeline creator has write access to the target table.
     *
     * @param context     the security context of the pipeline creator
     * @param targetTable the fully qualified target table name (catalog.schema.table)
     * @throws SecurityException if the creator no longer has write access
     */
    void enforceWriteAccess(PipelineSecurityContext context, String targetTable);
}
