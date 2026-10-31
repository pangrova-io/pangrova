package org.pangrova.materialization.domain;

import org.pangrova.common.exception.DomainException;
import org.pangrova.common.exception.PangrovaErrorCode;

/**
 * Recoverable domain failure of the materialization kernel.
 *
 * <p>Every factory maps to a stable error code so that SQL clients, the
 * reconciler, and operators can classify failures without parsing messages.
 */
public class MaterializationException extends DomainException {

    /**
     * Creates an exception with an explicit error code.
     *
     * @param errorCode structured error code
     * @param message human-readable description
     */
    public MaterializationException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    /**
     * Creates an exception with an explicit error code and cause.
     *
     * @param errorCode structured error code
     * @param message human-readable description
     * @param cause underlying cause
     */
    public MaterializationException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    /**
     * Creates a not-found failure.
     *
     * @param object missing object
     * @return domain exception
     */
    public static MaterializationException notFound(MaterializationId object) {
        return new MaterializationException(
                PangrovaErrorCode.MATERIALIZATION_OBJECT_NOT_FOUND,
                "Materialization not found: " + object.canonicalName());
    }

    /**
     * Creates an illegal state transition failure.
     *
     * @param message transition description
     * @return domain exception
     */
    public static MaterializationException invalidStateTransition(String message) {
        return new MaterializationException(
                PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION, message);
    }

    /**
     * Creates an invalid definition failure.
     *
     * @param message validation detail
     * @return domain exception
     */
    public static MaterializationException invalidDefinition(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_DEFINITION_INVALID, message);
    }

    /**
     * Creates a stale-fence failure.
     *
     * @param message fence detail
     * @return domain exception
     */
    public static MaterializationException fenceRejected(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_FENCE_REJECTED, message);
    }

    /**
     * Creates a head CAS conflict failure.
     *
     * @param message conflict detail
     * @return domain exception
     */
    public static MaterializationException concurrentPublication(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_CONCURRENT_PUBLICATION, message);
    }

    /**
     * Creates an evidence mismatch failure.
     *
     * @param message evidence detail
     * @return domain exception
     */
    public static MaterializationException evidenceMismatch(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_EVIDENCE_MISMATCH, message);
    }

    /**
     * Creates a missing-capability failure.
     *
     * @param message capability detail
     * @return domain exception
     */
    public static MaterializationException capabilityUnavailable(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_CAPABILITY_UNAVAILABLE, message);
    }

    /**
     * Creates a missing-capability failure with its cause.
     *
     * @param message capability detail
     * @param cause underlying connector failure
     * @return domain exception
     */
    public static MaterializationException capabilityUnavailable(String message, Throwable cause) {
        return new MaterializationException(
                PangrovaErrorCode.MATERIALIZATION_CAPABILITY_UNAVAILABLE, message, cause);
    }

    /**
     * Creates an incremental-unsupported failure.
     *
     * @param message why no incremental strategy applies
     * @return domain exception
     */
    public static MaterializationException incrementalUnsupported(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_INCREMENTAL_UNSUPPORTED, message);
    }

    /**
     * Creates a reserved-namespace failure.
     *
     * @param message namespace detail
     * @return domain exception
     */
    public static MaterializationException reservedNamespace(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_RESERVED_NAMESPACE, message);
    }

    /**
     * Creates an unsafe-rewrite failure.
     *
     * @param message guard detail
     * @return domain exception
     */
    public static MaterializationException rewriteUnsafe(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_REWRITE_UNSAFE, message);
    }

    /**
     * Creates a policy-drift failure.
     *
     * @param message drift detail
     * @return domain exception
     */
    public static MaterializationException policyDrift(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_POLICY_DRIFT, message);
    }

    /**
     * Creates a repository failure.
     *
     * @param message repository detail
     * @param cause underlying storage failure
     * @return domain exception
     */
    public static MaterializationException repositoryFailure(String message, Throwable cause) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_REPOSITORY_FAILURE, message, cause);
    }

    /**
     * Creates an invalid write-capability failure.
     *
     * @param message capability detail
     * @return domain exception
     */
    public static MaterializationException writeCapabilityInvalid(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_WRITE_CAPABILITY_INVALID, message);
    }

    /**
     * Creates an expired-lease failure.
     *
     * @param message lease detail
     * @return domain exception
     */
    public static MaterializationException leaseExpired(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_LEASE_EXPIRED, message);
    }

    /**
     * Creates a dependency-conflict failure.
     *
     * @param message dependency detail
     * @return domain exception
     */
    public static MaterializationException dependencyConflict(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_DEPENDENCY_CONFLICT, message);
    }

    /**
     * Creates a generic request-rejection failure.
     *
     * @param message rejection detail
     * @return domain exception
     */
    public static MaterializationException requestRejected(String message) {
        return new MaterializationException(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED, message);
    }
}
