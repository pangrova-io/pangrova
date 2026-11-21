package org.pangrova.materialization.api.port;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Connector-specific verification of commit evidence and ancestry.
 *
 * <p>Verification is a mandatory gate before publication. Implementations
 * check that the exact commit fingerprint exists, that its ancestry reaches the
 * expected base generation, and that the committed manifest hash matches the
 * receipt. A failure must throw; it must never return an optimistic verdict.
 */
@Experimental
public interface PublicationVerifier {

    /**
     * Verifies a publication candidate.
     *
     * @param verification candidate, attempt, and evidence
     */
    void verify(PublicationVerification verification);
}
