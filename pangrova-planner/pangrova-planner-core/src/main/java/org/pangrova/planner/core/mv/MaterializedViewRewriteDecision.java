package org.pangrova.planner.core.mv;

import java.util.List;

/**
 * Outcome of one transparent materialized-view rewrite attempt.
 *
 * <p>The decision is diagnostic only: it never changes whether a rewrite is
 * applied. {@code EXPLAIN} renders it so a reader can distinguish a plan that
 * already reads a published generation from one that kept the base sources.
 *
 * @param applied whether the returned plan reads a published generation
 * @param views logical names of the substituted views, empty when not applied
 * @param reason why the rewrite was skipped, empty when applied
 */
public record MaterializedViewRewriteDecision(boolean applied, List<String> views, String reason) {

    public MaterializedViewRewriteDecision {
        views = views == null ? List.of() : List.copyOf(views);
        reason = reason == null ? "" : reason;
        if (applied && views.isEmpty()) {
            throw new IllegalArgumentException("an applied rewrite must name at least one view");
        }
        if (!applied && reason.isBlank()) {
            throw new IllegalArgumentException("a skipped rewrite requires a reason");
        }
    }

    /** Creates a decision for a substitution that was applied. */
    public static MaterializedViewRewriteDecision applied(List<String> views) {
        return new MaterializedViewRewriteDecision(true, views, "");
    }

    /** Creates a decision for a rewrite that was evaluated and rejected. */
    public static MaterializedViewRewriteDecision skipped(String reason) {
        return new MaterializedViewRewriteDecision(false, List.of(), reason);
    }

    /** Creates the decision used when no rewrite catalog is wired. */
    public static MaterializedViewRewriteDecision disabled() {
        return skipped("rewrite_disabled");
    }

    /**
     * Returns a stable one-line rendering for {@code EXPLAIN}.
     *
     * @return decision summary
     */
    public String describe() {
        return applied
                ? "materialized_view_rewrite: applied views=" + String.join(",", views)
                : "materialized_view_rewrite: skipped reason=" + reason;
    }
}
