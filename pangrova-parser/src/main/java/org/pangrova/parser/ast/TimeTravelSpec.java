package org.pangrova.parser.ast;

/**
 * Represents a time travel specification for table references.
 *
 * <p>Supports system time travel (FOR SYSTEM_TIME AS OF), system version travel
 * (FOR SYSTEM_VERSION/VERSION AS OF), and branch/tag version travel
 * (VERSION AS OF BRANCH/TAG 'name').</p>
 */
public record TimeTravelSpec(Type type, Expression expression, String refName) {

    public enum Type {
        SYSTEM_TIME,
        SYSTEM_VERSION,
        BRANCH,
        TAG
    }

    /**
     * Creates a time travel spec with an expression (SYSTEM_TIME or SYSTEM_VERSION).
     */
    public TimeTravelSpec(Type type, Expression expression) {
        this(type, expression, null);
    }

    /**
     * Creates a branch time travel spec with a reference name.
     */
    public static TimeTravelSpec branch(String branchName) {
        return new TimeTravelSpec(Type.BRANCH, null, branchName);
    }

    /**
     * Creates a tag time travel spec with a reference name.
     */
    public static TimeTravelSpec tag(String tagName) {
        return new TimeTravelSpec(Type.TAG, null, tagName);
    }
}
