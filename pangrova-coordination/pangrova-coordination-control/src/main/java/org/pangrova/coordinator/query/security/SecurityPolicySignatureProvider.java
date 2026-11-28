package org.pangrova.coordinator.query.security;

import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.planner.analysis.PlannedQuery;
import org.pangrova.security.api.identity.UserIdentity;

import java.util.Objects;
import java.util.TreeSet;

/**
 * Computes a deterministic signature of the security context that produced an
 * already-authorized and security-rewritten query plan.
 *
 * <p>The secured plan digest is authoritative: it contains the effective RLS
 * predicates and column-mask projections after policy application. Group names
 * alone are deliberately insufficient because a policy can change without any
 * membership change.</p>
 */
public final class SecurityPolicySignatureProvider {

    private SecurityPolicySignatureProvider() {}

    /**
     * Computes the policy signature for the given user.
     *
     * <p>The signature incorporates:
     * <ul>
     *   <li>The user's principal name</li>
     *   <li>The user's sorted group membership (determines which RLS/mask policies apply)</li>
     *   <li>For each monitored table: the effective RLS filter expression (if any)</li>
     * </ul>
     *
     * @param user the user identity; may be null (anonymous)
     * @param securedPlan the already-authorized plan after RLS and mask rewriting
     * @return a deterministic SHA-256 hex digest for the effective security plan
     */
    public static String computeEffectiveSignature(UserIdentity user, PlannedQuery securedPlan) {
        Objects.requireNonNull(securedPlan, "securedPlan must not be null");
        StringBuilder sb = new StringBuilder();
        if (user == null) {
            sb.append("principal=anonymous;groups=[];");
        } else {
            sb.append("principal=").append(user.principal()).append(';');
            sb.append("groups=").append(new TreeSet<>(user.groups())).append(';');
        }
        sb.append("effectivePlan=").append(securedPlan.effectivePlanDigest());
        return Sha256Digests.sha256Hex(sb.toString());
    }
}
