package org.pangrova.ai.api.invocation;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Engine-owned authorization and resource admission for one physical provider send.
 * May be shared by provider batch threads. Every send, including a permitted retry,
 * requires a new admission; this port does not authorize replay of an unknown outcome.
 */
@Experimental
@FunctionalInterface
public interface AttemptAdmission {
    /**
     * Reserves concurrency and charges encoded entity bytes before transport submission.
     * May block for capacity within the invocation deadline. Revocation, deadline and
     * budget failures reject the send with a typed AI invocation error. The caller owns
     * the returned attempt and must close it after local transport delivery ends.
     *
     * @param encodedRequestBytes nonnegative encoded entity size, excluding network framing
     * @return live attempt; never null
     */
    PhysicalAttempt open(long encodedRequestBytes);
}
