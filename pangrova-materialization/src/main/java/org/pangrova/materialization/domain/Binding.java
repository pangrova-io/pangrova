package org.pangrova.materialization.domain;

/**
 * Normalized definition binding produced by the planner adapter.
 *
 * <p>{@code canonicalSql} is a parameter-free, normalized form of the
 * definition query. {@code planFingerprint} identifies the planner contract
 * and resolved object versions used to validate that fingerprint. Both are
 * persisted so that a revision can be re-planned deterministically.
 *
 * @param canonicalSql normalized definition SQL
 * @param planFingerprint planner-computed fingerprint of the bound plan
 */
public record Binding(String canonicalSql, String planFingerprint) {

    public Binding {
        canonicalSql = Require.text(canonicalSql, "canonicalSql");
        planFingerprint = Require.text(planFingerprint, "planFingerprint");
    }
}
