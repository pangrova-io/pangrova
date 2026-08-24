package org.pangrova.planner.cache;

import org.pangrova.common.util.DigestUtils;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Cache key for the optimized {@link org.pangrova.planner.PlanResult} cache.
 *
 * <p>Two queries hit the same plan iff their normalized SQL text and session
 * properties digest both match. Normalization collapses whitespace and lowercases
 * the SQL so cosmetic differences ("SELECT 1" vs "select  1") share a slot.</p>
 *
 * <p>Unlike {@link CacheKey} (the result-cache key), the plan-cache key does
 * <strong>not</strong> include tenant/principal/policy dimensions. The optimized
 * RelNode is independent of who runs the query — RLS, column masking, and
 * access control are layered on after planning by
 * {@code SecurityPolicyEnforcer}.</p>
 *
 * <p>Schema/statistics changes are handled by listener-driven invalidation
 * (see {@link PlanCache#invalidateByTable(String)}) rather than encoded in the
 * key, keeping the key cheap to compute on every plan() call.</p>
 */
public final class PlanCacheKey {

    private final String normalizedSql;
    private final String sessionPropertiesDigest;
    private final String keyString;

    private PlanCacheKey(String normalizedSql, String sessionPropertiesDigest) {
        this.normalizedSql = Objects.requireNonNull(normalizedSql, "normalizedSql");
        this.sessionPropertiesDigest = sessionPropertiesDigest != null ? sessionPropertiesDigest : "";
        this.keyString = DigestUtils.sha256Hex(this.normalizedSql + "|" + this.sessionPropertiesDigest);
    }

    /** Build a key from raw SQL with no session properties. */
    public static PlanCacheKey of(String sql) {
        return of(sql, null);
    }

    /** Build a key from raw SQL and a map of session properties (sorted, digested). */
    public static PlanCacheKey of(String sql, Map<String, String> sessionProperties) {
        return new PlanCacheKey(normalizeSql(sql), digestProperties(sessionProperties));
    }

    public String normalizedSql() {
        return normalizedSql;
    }

    public String sessionPropertiesDigest() {
        return sessionPropertiesDigest;
    }

    /** Deterministic SHA-256 hex string suitable for map keys / external stores. */
    public String keyString() {
        return keyString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PlanCacheKey that)) return false;
        return normalizedSql.equals(that.normalizedSql)
                && sessionPropertiesDigest.equals(that.sessionPropertiesDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(normalizedSql, sessionPropertiesDigest);
    }

    @Override
    public String toString() {
        return "PlanCacheKey{sql=" + normalizedSql + ", propsDigest=" + sessionPropertiesDigest + "}";
    }

    static String normalizeSql(String sql) {
        if (sql == null) return "";
        return sql.strip().replaceAll("\\s+", " ").toLowerCase();
    }

    static String digestProperties(Map<String, String> props) {
        if (props == null || props.isEmpty()) {
            return "";
        }
        TreeMap<String, String> sorted = new TreeMap<>(props);
        return DigestUtils.sha256Hex(sorted.toString());
    }
}
