package org.pangrova.materialization.api.identity;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Strategy identifier used by the refresh strategy registry.
 *
 * @param value canonical uppercase identifier
 */
public record StrategyId(String value) {

    /** Full recomputation. */
    public static final StrategyId FULL = new StrategyId("FULL");

    /** Append-only incremental merge. */
    public static final StrategyId APPEND_ONLY = new StrategyId("APPEND_ONLY");

    /** Affected-partition replacement. */
    public static final StrategyId PARTITION_REPLACE = new StrategyId("PARTITION_REPLACE");

    /** Affected-key recomputation. */
    public static final StrategyId AFFECTED_KEY = new StrategyId("AFFECTED_KEY");

    /** Change-data-capture application. */
    public static final StrategyId CDC = new StrategyId("CDC");

    public StrategyId {
        value = requireText(value, "strategyId");
        if (!value.matches("[A-Z][A-Z0-9_]*")) {
            throw new IllegalArgumentException("strategyId must match [A-Z][A-Z0-9_]*: " + value);
        }
    }
}
