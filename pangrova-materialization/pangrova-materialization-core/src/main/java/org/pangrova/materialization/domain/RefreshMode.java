package org.pangrova.materialization.domain;

/**
 * Configured refresh mode of a materialization object.
 *
 * <p>{@link #INCREMENTAL} never degrades to {@link #FULL} implicitly. A
 * definition that cannot resolve a compatible incremental strategy fails with
 * {@code INCREMENTAL_UNSUPPORTED}; changing modes requires an explicit
 * {@code SET REFRESH_MODE} followed by a full refresh.
 */
public enum RefreshMode {

    /** Every refresh recomputes the full result. */
    FULL,

    /** Every refresh must apply a proven change set. */
    INCREMENTAL;

    /**
     * Returns whether the mode accepts a resolved refresh kind.
     *
     * @param kind resolved refresh kind
     * @return true when the kind is compatible with this mode
     */
    public boolean accepts(RefreshKind kind) {
        return switch (this) {
            case FULL -> !kind.isIncremental();
            case INCREMENTAL -> kind.isIncremental();
        };
    }
}
