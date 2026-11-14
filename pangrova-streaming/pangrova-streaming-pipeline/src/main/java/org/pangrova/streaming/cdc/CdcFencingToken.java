package org.pangrova.streaming.cdc;

import org.pangrova.streaming.exception.StaleFenceException;

/**
 * Application-level fencing token for the CDC write/commit path.
 *
 * <p>During non-graceful leader changes, a zombie (deposed) pipeline may still be
 * executing after a new leader has taken over. Without fencing, the zombie can
 * write stale data or commit offsets, causing corruption or duplicates.</p>
 *
 * <p>This interface allows the pipeline loop to validate that its assigned epoch
 * is still current before performing any write or commit operation. Implementations
 * typically query the distributed lease infrastructure to check whether the epoch
 * has advanced.</p>
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #validate(long)} is called before each write batch and each commit.</li>
 *   <li>If the pipeline's epoch is still current, the method returns normally.</li>
 *   <li>If a newer epoch exists (new leader elected), the method throws
 *       {@link StaleFenceException}.</li>
 * </ul>
 *
 * <h2>Implementations</h2>
 * <ul>
 *   <li>Lease-backed validation must be supplied by process assembly. This contract
 *       does not construct or select a lease provider.</li>
 *   <li>Test: {@link #NOOP} for pipelines that do not require fencing.</li>
 * </ul>
 *
 * @see StaleFenceException
 * @see CdcPipelineLoop
 */
@FunctionalInterface
public interface CdcFencingToken {

    /**
     * A no-op fencing token that never rejects. Used in single-node deployments
     * and tests where leader election is not active.
     */
    CdcFencingToken NOOP = epoch -> {};

    /**
     * Validates that the given pipeline epoch is still current.
     *
     * @param pipelineEpoch the epoch assigned to this pipeline instance at startup
     * @throws StaleFenceException if a newer epoch exists (zombie detection)
     */
    void validate(long pipelineEpoch) throws StaleFenceException;
}
