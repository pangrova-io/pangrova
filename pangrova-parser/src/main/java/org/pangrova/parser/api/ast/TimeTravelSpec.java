package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.TimeTravelType;

/**
 * Represents a time travel specification for table references.
 *
 * <p>Supports system time travel (FOR SYSTEM_TIME AS OF), system version travel
 * (FOR SYSTEM_VERSION/VERSION AS OF), and branch/tag version travel
 * (VERSION AS OF BRANCH/TAG 'name').</p>
 */
public record TimeTravelSpec(TimeTravelType type, Expression expression, String refName) {

    /**
     * Creates a time travel spec with an expression (SYSTEM_TIME or SYSTEM_VERSION).
     */
    public TimeTravelSpec(TimeTravelType type, Expression expression) {
        this(type, expression, null);
    }

    /**
     * Creates a branch time travel spec with a reference name.
     */
    public static TimeTravelSpec branch(String branchName) {
        return new TimeTravelSpec(TimeTravelType.BRANCH, null, branchName);
    }

    /**
     * Creates a tag time travel spec with a reference name.
     */
    public static TimeTravelSpec tag(String tagName) {
        return new TimeTravelSpec(TimeTravelType.TAG, null, tagName);
    }
}
