package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.WindowFrameBoundType;
import org.pangrova.parser.api.enums.WindowFrameType;

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
    public record WindowFrame(WindowFrameType frameType, FrameBound lowerBound, FrameBound upperBound) {

        /** Backward-compatible constructor defaulting to ROWS. */
        public WindowFrame(FrameBound lowerBound, FrameBound upperBound) {
            this(WindowFrameType.ROWS, lowerBound, upperBound);
        }
    }

    /**
     * Represents a single frame bound (e.g., UNBOUNDED PRECEDING, CURRENT ROW).
     */
    public record FrameBound(WindowFrameBoundType type, Optional<Expression> offset) {

        public FrameBound(WindowFrameBoundType type) {
            this(type, Optional.empty());
        }
    }

}
