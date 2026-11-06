package org.pangrova.planner.core.cache;

import java.util.Map;
import java.util.TreeMap;

/**
 * Computes a semantic fingerprint for a query including security context.
 * Different security contexts always produce different fingerprints.
 */
public class QueryFingerprintCalculator {

    /**
     * Calculate fingerprint from SQL and query context.
     */
    public QueryFingerprint calculate(String sql, QueryContext context) {
        String normalizedSql = normalizeSql(sql);
        String baseFingerprint = QueryFingerprint.sha256(normalizedSql);
        String contextHash = computeContextHash(context);

        return new QueryFingerprint(
                baseFingerprint,
                contextHash,
                context.dataVersion(),
                context.tenantId(),
                context.userPrincipal(),
                context.activeRolesHash(),
                context.rlsPolicyHash(),
                context.columnMaskHash());
    }

    private String normalizeSql(String sql) {
        return CacheKey.normalizeSql(sql);
    }

    private String computeContextHash(QueryContext ctx) {
        Map<String, String> sorted = new TreeMap<>(
                ctx.sessionProperties() != null ? ctx.sessionProperties() : Map.of());
        String raw = String.join("|",
                nullSafe(ctx.catalog()),
                nullSafe(ctx.schema()),
                nullSafe(ctx.defaultSchemaPath()),
                sorted.toString());
        return QueryFingerprint.sha256(raw);
    }

    private static String nullSafe(String value) {
        return value != null ? value : "";
    }
}
