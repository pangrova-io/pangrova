package org.pangrova.streaming.cdc;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable security context for CDC pipeline execution.
 *
 * <p>Carries the identity of the user who created the streaming pipeline.
 * This context is propagated to the pipeline thread and made available to
 * the write sink via {@link PipelineSecurityContextHolder}, ensuring that
 * RLS (row-level security), column masking, and authorization checks can
 * be applied based on the creating user's identity — not a privileged
 * system identity that would silently bypass security policies.</p>
 *
 * <h3>Design Decision: Definer-Identity Model</h3>
 *
 * <p>CDC pipelines run continuously without a live interactive session.
 * The security model follows the <em>definer-identity</em> pattern (similar
 * to SQL SECURITY DEFINER views): the pipeline executes with the permissions
 * of the user who created it. This means:</p>
 * <ul>
 *   <li>If the creator's permissions are revoked, the pipeline loses access</li>
 *   <li>RLS policies that reference {@code current_user} resolve to the creator</li>
 *   <li>Column masking applies based on the creator's group membership</li>
 * </ul>
 *
 * <p>A {@link #SYSTEM} context is provided for internal/bootstrap pipelines
 * that intentionally run with elevated privileges. Usage of SYSTEM must be
 * explicit and auditable — never a silent default.</p>
 *
 * @param principal       the authenticated principal name of the pipeline creator
 * @param groups          group memberships for RBAC and policy evaluation
 * @param attributes      additional identity attributes (e.g., tenant, department)
 * @param authenticatedAt when the creator's identity was verified
 *
 * @see PipelineSecurityContextHolder
 * @see CdcPipelineLoop
 */
public record PipelineSecurityContext(
        String principal,
        Set<String> groups,
        Map<String, String> attributes,
        Instant authenticatedAt
) {

    /**
     * System-level security context for internal pipelines that intentionally
     * bypass user-level security policies. Usage must be explicit and logged.
     */
    public static final PipelineSecurityContext SYSTEM = new PipelineSecurityContext(
            "__system__", Set.of("__system__"), Map.of("internal", "true"), Instant.EPOCH);

    public PipelineSecurityContext {
        Objects.requireNonNull(principal, "principal must not be null");
        groups = groups == null ? Set.of() : Set.copyOf(groups);
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        if (authenticatedAt == null) {
            authenticatedAt = Instant.now();
        }
    }

    /**
     * Creates a security context from a principal name with default empty groups/attributes.
     * Used when only the creator identity string is available (e.g., from StreamState).
     *
     * @param principal the creator's principal name
     * @return a security context for the given principal
     */
    public static PipelineSecurityContext forPrincipal(String principal) {
        Objects.requireNonNull(principal, "principal must not be null");
        return new PipelineSecurityContext(principal, Set.of(), Map.of(), Instant.now());
    }

    /**
     * Returns whether this context represents a system/internal pipeline.
     */
    public boolean isSystemContext() {
        return "__system__".equals(principal);
    }
}
