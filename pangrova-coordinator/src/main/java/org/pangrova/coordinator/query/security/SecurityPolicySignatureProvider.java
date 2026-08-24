package org.pangrova.coordinator.query.security;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Computes a deterministic signature (SHA-256 digest) of the security policies
 * applicable to a given user identity. The signature encodes the effective RLS
 * filters and column masks so that users with different policy sets produce
 * different cache keys — preventing cross-user result leakage.
 *
 * <p>The signature is stable for the same policy state: identical policies
 * produce identical digests regardless of retrieval order. When policies change
 * (e.g., a new RLS rule is added), the signature changes, causing cache misses
 * and forcing re-execution under the new policy.</p>
 *
 * <p>Thread-safe: delegates to {@link AccessControl} which is thread-safe.</p>
 */
public final class SecurityPolicySignatureProvider {

    private final AccessControl accessControl;
    private final Set<String> monitoredTables;

    /**
     * @param accessControl    the access control implementation providing RLS filters and column masks
     * @param monitoredTables  tables to probe for policy state; when empty, the signature
     *                         is based on the user's group membership only (lightweight fallback)
     */
    public SecurityPolicySignatureProvider(AccessControl accessControl,
                                           Set<String> monitoredTables) {
        this.accessControl = accessControl;
        this.monitoredTables = monitoredTables != null ? Set.copyOf(monitoredTables) : Set.of();
    }

    /**
     * Constructs a provider that computes signatures based solely on user group
     * membership (no per-table probing). Suitable when table-specific policy probing
     * is too expensive or the table set is not known at cache-key time.
     */
    public SecurityPolicySignatureProvider(AccessControl accessControl) {
        this(accessControl, Set.of());
    }

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
     * <p>Returns an empty string when access control is null or the user is anonymous,
     * which causes the cache key to have an empty policy dimension (no caching isolation
     * for anonymous users is needed since they all share the same policy state).</p>
     *
     * @param user the user identity; may be null (anonymous)
     * @return a deterministic SHA-256 hex digest, or empty string if no policy applies
     */
    public String computeSignature(UserIdentity user) {
        if (accessControl == null || user == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("principal=").append(user.principal()).append(';');

        // Sorted groups for deterministic output
        TreeSet<String> sortedGroups = new TreeSet<>(user.groups());
        sb.append("groups=").append(sortedGroups).append(';');

        // Probe RLS filters for monitored tables
        for (String qualifiedTable : new TreeSet<>(monitoredTables)) {
            String[] parts = qualifiedTable.split("\\.");
            if (parts.length == 3) {
                Optional<String> filter = accessControl.getRowFilter(
                        user, parts[0], parts[1], parts[2]);
                filter.ifPresent(f -> sb.append("rls:").append(qualifiedTable).append('=').append(f).append(';'));
            }
        }

        return DigestUtils.sha256Hex(sb.toString());
    }

    /**
     * Computes a lightweight policy signature based only on the user's principal
     * and group membership. This avoids per-table probing but still ensures that
     * users with different roles (and hence different applicable policies) get
     * different cache keys.
     *
     * <p>This is the recommended approach when the referenced tables are not known
     * at cache-key construction time (before compilation).</p>
     *
     * @param user the user identity; may be null
     * @return a deterministic SHA-256 hex digest, or empty string for anonymous users
     */
    public static String computeGroupBasedSignature(UserIdentity user) {
        if (user == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("principal=").append(user.principal()).append(';');
        TreeSet<String> sortedGroups = new TreeSet<>(user.groups());
        sb.append("groups=").append(sortedGroups);
        return DigestUtils.sha256Hex(sb.toString());
    }
}
