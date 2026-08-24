package org.pangrova.planner.cache;

import org.pangrova.common.util.DigestUtils;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Immutable cache key incorporating tenant/user/session/policy isolation dimensions.
 * Two cache keys are equal iff all dimensions match, preventing cross-tenant result leakage.
 *
 * <p>The {@link #keyString()} produces a deterministic SHA-256 digest suitable for
 * use as a map key in both L1 (JVM heap) and L2 (external store) cache layers.</p>
 */
public final class CacheKey {

    private final String normalizedSql;
    private final String tenantId;
    private final String principal;
    private final String sessionPropertiesDigest;
    private final String policyVersion;
    private final String keyString;

    private CacheKey(String normalizedSql,
                     String tenantId,
                     String principal,
                     String sessionPropertiesDigest,
                     String policyVersion) {
        this.normalizedSql = Objects.requireNonNull(normalizedSql, "normalizedSql must not be null");
        this.tenantId = nullSafe(tenantId);
        this.principal = nullSafe(principal);
        this.sessionPropertiesDigest = nullSafe(sessionPropertiesDigest);
        this.policyVersion = nullSafe(policyVersion);
        this.keyString = computeKeyString();
    }

    /**
     * Build a CacheKey from raw session fields.
     * SQL is normalized (trimmed, collapsed whitespace, lowercased).
     * Session properties are sorted and digested for deterministic hashing.
     */
    public static CacheKey of(String sql,
                              String tenantId,
                              String principal,
                              Map<String, String> sessionProperties,
                              String policyVersion) {
        String normalizedSql = normalizeSql(sql);
        String propsDigest = digestSessionProperties(sessionProperties);
        return new CacheKey(normalizedSql, tenantId, principal, propsDigest, policyVersion);
    }

    public String normalizedSql() {
        return normalizedSql;
    }

    public String tenantId() {
        return tenantId;
    }

    public String principal() {
        return principal;
    }

    public String sessionPropertiesDigest() {
        return sessionPropertiesDigest;
    }

    public String policyVersion() {
        return policyVersion;
    }

    /**
     * Deterministic string representation suitable for L2 cache store keys.
     */
    public String keyString() {
        return keyString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CacheKey that)) return false;
        return normalizedSql.equals(that.normalizedSql)
                && tenantId.equals(that.tenantId)
                && principal.equals(that.principal)
                && sessionPropertiesDigest.equals(that.sessionPropertiesDigest)
                && policyVersion.equals(that.policyVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(normalizedSql, tenantId, principal, sessionPropertiesDigest, policyVersion);
    }

    @Override
    public String toString() {
        return "CacheKey{sql=" + normalizedSql
                + ", tenant=" + tenantId
                + ", principal=" + principal
                + ", policyVersion=" + policyVersion + "}";
    }

    private String computeKeyString() {
        String combined = String.join("|",
                normalizedSql, tenantId, principal, sessionPropertiesDigest, policyVersion);
        return DigestUtils.sha256Hex(combined);
    }

    static String normalizeSql(String sql) {
        if (sql == null) return "";
        return sql.strip().replaceAll("\\s+", " ").toLowerCase();
    }

    static String digestSessionProperties(Map<String, String> props) {
        if (props == null || props.isEmpty()) {
            return "";
        }
        TreeMap<String, String> sorted = new TreeMap<>(props);
        return DigestUtils.sha256Hex(sorted.toString());
    }

    private static String nullSafe(String value) {
        return value != null ? value : "";
    }
}
