package org.pangrova.planner.stability;

import org.pangrova.common.util.DigestUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

/**
 * Computes canonical hashes of an SQL string ("query hash") and an optimized
 * RelNode tree ("plan hash"). Used by the plan stability subsystem
 * ({@link PlanCaptureRegistry}, {@link PlanBaselineRegistry}) to detect plan
 * regressions across runs of the same query.
 *
 * <h2>Query hash</h2>
 * A whitespace-collapsed lowercase SHA-256 of the SQL text. Two queries that
 * differ only in formatting share the same query hash. The same normalization
 * rule used by {@link org.pangrova.planner.cache.PlanCacheKey} so the two
 * subsystems remain consistent.
 *
 * <h2>Plan hash</h2>
 * SHA-256 of the structural digest produced by {@link RelWriterImpl} at the
 * {@code DIGEST_ATTRIBUTES} level <em>without</em> per-node IDs. The digest
 * captures operator types, attributes (predicates, join conditions, sort keys,
 * table names) and the input tree, but excludes the volatile internal IDs that
 * Calcite assigns during planning. Two structurally identical optimized plans
 * — even if produced by different plan() calls — therefore share the same plan
 * hash.
 *
 * <p>This is the same canonical-form approach used by Oracle's SQL Plan
 * Management ({@code PLAN_HASH_VALUE}) and PostgreSQL's pg_stat_statements
 * fingerprinting: hash the structure, not the rendering.</p>
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class PlanHasher {

    private PlanHasher() {}

    /**
     * Returns a 64-character lowercase hex SHA-256 of the normalized SQL text.
     * Whitespace is collapsed and the text is lowercased before hashing so
     * cosmetic differences ("SELECT 1" vs "select  1") share a hash.
     */
    public static String queryHash(String sql) {
        Objects.requireNonNull(sql, "sql");
        return DigestUtils.sha256Hex(normalizeSql(sql));
    }

    /**
     * Returns a 64-character lowercase hex SHA-256 of the canonical structural
     * digest of {@code plan}. Per-node IDs are excluded so the hash is stable
     * across plan() calls that produce structurally identical trees.
     */
    public static String planHash(RelNode plan) {
        Objects.requireNonNull(plan, "plan");
        return DigestUtils.sha256Hex(canonicalDigest(plan));
    }

    /**
     * Returns the canonical multi-line digest used as input to
     * {@link #planHash(RelNode)}. Exposed for diagnostic logging when a
     * baseline mismatch is detected — comparing the two strings is the
     * fastest way to identify which operator changed.
     */
    public static String canonicalDigest(RelNode plan) {
        Objects.requireNonNull(plan, "plan");
        StringWriter sw = new StringWriter();
        RelWriterImpl writer = new RelWriterImpl(
                new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, false);
        plan.explain(writer);
        return sw.toString().trim();
    }

    private static String normalizeSql(String sql) {
        return sql.strip().replaceAll("\\s+", " ").toLowerCase();
    }
}
