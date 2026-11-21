package org.pangrova.persistence.security.api.grant;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.security.api.grant.PrivilegeMutation;
import org.pangrova.security.api.grant.PrivilegeMutationReceipt;
import org.pangrova.security.api.identity.UserIdentity;

/**
 * Atomic privilege authority boundary. Implementations are thread-safe and must serialize current
 * role resolution, delegation authorization, mutation, dependency cleanup, revision and durable audit.
 * No authorization decision made before this operation substitutes for its current authority check.
 *
 * <p>Only OWNER or a containing privilege WITH GRANT OPTION can delegate. OWNER itself cannot be
 * delegated or revoked here. Non-owners revoke only grants they issued; owners may revoke all exact
 * matching grants. RESTRICT rejects dependencies; CASCADE removes their transitive closure. A grant
 * records one specific supporting grant, not an inferred privilege union. Role membership is checked
 * at issuance; subsequent membership removal does not withdraw grants already issued as that role.
 *
 * <p>Same request ID, actor principal and payload returns the recorded receipt, even after authority
 * changes. A different actor or payload is rejected. Authentication remains the caller's responsibility.
 * Commit acknowledgement loss is reported as PrivilegeMutationOutcomeUnknownException. The caller
 * confirms through same-ID replay; no background retry, fallback authority or partial success is allowed.
 * This synchronous operation owns its transaction and connection; it does not own the shared datasource.
 */
@Experimental
public interface PrivilegeMutationRepository {
    /**
     * Confirms a committed receipt belonging to the authenticated principal. Empty means unconfirmed
     * (including still in flight), never proof of rollback. Other principals cannot discover the receipt.
     * Storage failures propagate instead of returning empty. No current delegation privilege is required
     * to inspect one's own prior outcome after a revoke.
     */
    java.util.Optional<PrivilegeMutationReceipt> findReceipt(UserIdentity actor, java.util.UUID requestId);

    /**
     * Synchronous mutation. Caller cancellation or timeout does not prove rollback; confirm the original request ID.
     */
    PrivilegeMutationReceipt apply(UserIdentity actor, PrivilegeMutation mutation);
}
