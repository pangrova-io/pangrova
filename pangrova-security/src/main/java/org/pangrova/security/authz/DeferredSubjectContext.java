package org.pangrova.security.authz;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.pangrova.spi.security.UserIdentity;

/** Identifies a persisted principal whose ephemeral authentication context is unavailable. */
public final class DeferredSubjectContext {
    private static final String DEFERRED_ATTRIBUTE = "pangrova.internal.deferred_subject";

    private DeferredSubjectContext() {}

    /**
     * Restores only durable subject facts; roles must still be resolved from the current grant store.
     * The internal attribute only adds restrictions and never grants a permission.
     *
     * @param principal persisted authenticated principal
     * @param tenantId persisted tenant, or null when the caller carries tenant scope separately
     * @return subject without reconstructed groups or authentication attributes
     */
    public static UserIdentity fromPrincipal(String principal, String tenantId) {
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("Deferred subject requires a principal");
        }
        Map<String, String> attributes = tenantId == null
                ? Map.of(DEFERRED_ATTRIBUTE, "true")
                : Map.of(DEFERRED_ATTRIBUTE, "true", "tenant_id", tenantId);
        return new UserIdentity(principal, Set.of(), attributes, Instant.EPOCH);
    }

    /** Rejects policies that need authentication context unavailable to a deferred execution. */
    public static void requireStable(UserIdentity subject, String expression) {
        if (!"true".equals(subject.attributes().get(DEFERRED_ATTRIBUTE))) return;
        String normalized = expression.toLowerCase(Locale.ROOT).replaceAll("\\s+", "");
        if (normalized.contains("current_groups()") || normalized.contains("user_attribute(")) {
            throw new AuthorizationDeniedException(
                    "Deferred policy requires a current identity-context resolver; resubmit after reauthorization");
        }
    }
}
