package org.pangrova.materialization.api.rewrite;

/**
 * Decision of one rewrite guard.
 *
 * @param allowed whether the rewrite may proceed
 * @param reason explanation; empty when allowed
 */
public record RewriteDecision(boolean allowed, String reason) {

    public RewriteDecision {
        reason = reason == null ? "" : reason;
        if (!allowed && reason.isBlank()) {
            throw new IllegalArgumentException("a skipped rewrite requires a reason");
        }
    }

    /**
     * Returns an allowed decision.
     *
     * @return allowed decision
     */
    public static RewriteDecision allow() {
        return new RewriteDecision(true, "");
    }

    /**
     * Returns a skip decision.
     *
     * @param reason why the rewrite is unsafe or ineligible
     * @return skip decision
     */
    public static RewriteDecision skip(String reason) {
        return new RewriteDecision(false, reason);
    }
}
