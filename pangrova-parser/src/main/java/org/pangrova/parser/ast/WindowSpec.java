package org.pangrova.parser.ast;

import java.util.List;
import java.util.Optional;

/**
 * Represents a window specification (OVER clause) for window functions.
 * Supports both inline window definitions and named window references.
 */
public record WindowSpec(
        Optional<String> windowName,
        List<Expression> partitionBy,
        List<SortItem> orderBy,
        Optional<WindowFrame> frame) {

    public WindowSpec {
        partitionBy = List.copyOf(partitionBy);
        orderBy = List.copyOf(orderBy);
    }

    /** Inline window spec (no named reference). */
    public WindowSpec(List<Expression> partitionBy, List<SortItem> orderBy, Optional<WindowFrame> frame) {
        this(Optional.empty(), partitionBy, orderBy, frame);
    }

    /** Backward-compatible constructor without frame. */
    public WindowSpec(List<Expression> partitionBy, List<SortItem> orderBy) {
        this(Optional.empty(), partitionBy, orderBy, Optional.empty());
    }

    /** Named window reference (OVER window_name). */
    public static WindowSpec ofNamedReference(String windowName) {
        return new WindowSpec(Optional.of(windowName), List.of(), List.of(), Optional.empty());
    }

    /**
     * Represents a window frame (ROWS or RANGE BETWEEN ... AND ...).
     */
    public record WindowFrame(FrameType frameType, FrameBound lowerBound, FrameBound upperBound) {

        /** Backward-compatible constructor defaulting to ROWS. */
        public WindowFrame(FrameBound lowerBound, FrameBound upperBound) {
            this(FrameType.ROWS, lowerBound, upperBound);
        }
    }

    /**
     * Frame type: ROWS (physical offset) or RANGE (logical/value offset).
     */
    public enum FrameType {
        ROWS,
        RANGE
    }

    /**
     * Represents a single frame bound (e.g., UNBOUNDED PRECEDING, CURRENT ROW).
     */
    public record FrameBound(FrameBoundType type, Optional<Expression> offset) {

        public FrameBound(FrameBoundType type) {
            this(type, Optional.empty());
        }
    }

    public enum FrameBoundType {
        UNBOUNDED_PRECEDING,
        UNBOUNDED_FOLLOWING,
        CURRENT_ROW,
        EXPR_PRECEDING,
        EXPR_FOLLOWING
    }
}
