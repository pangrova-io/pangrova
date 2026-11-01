package org.pangrova.streaming.watermark;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Propagates watermarks through streaming operators with correct semantics.
 *
 * <p>Watermark propagation rules by operator type:</p>
 * <ul>
 *   <li><b>Union</b>: output watermark = min(input watermarks). The union cannot
 *       advance beyond the slowest input, as late data from any input would be
 *       incorrectly dropped.</li>
 *   <li><b>Join</b>: output watermark = min(left watermark, right watermark).
 *       Both sides must have advanced before the join output can advance.</li>
 *   <li><b>Aggregation</b>: output watermark = input watermark - allowed lateness.
 *       The aggregation holds back the watermark by the configured late data tolerance.</li>
 *   <li><b>Filter/Project</b>: output watermark = input watermark (pass-through).
 *       These operators do not affect event time.</li>
 * </ul>
 *
 * <p>Concurrency strategy: this class is stateless and thread-safe. All state
 * is managed by the caller (typically {@link WatermarkTracker}).</p>
 */
@ThreadSafe
public final class WatermarkPropagator {

    private static final Logger LOG = LoggerFactory.getLogger(WatermarkPropagator.class);

    private WatermarkPropagator() {
        // Utility class with static methods
    }

    /**
     * Computes the output watermark for a union operator.
     * Union semantics: min of all input watermarks.
     *
     * @param inputWatermarks watermarks from all union inputs
     * @return the minimum watermark, or {@link Instant#MIN} if inputs are empty
     */
    public static Instant propagateUnion(List<Instant> inputWatermarks) {
        Objects.requireNonNull(inputWatermarks, "inputWatermarks must not be null");
        if (inputWatermarks.isEmpty()) {
            return Instant.MIN;
        }
        Instant min = inputWatermarks.get(0);
        for (int i = 1; i < inputWatermarks.size(); i++) {
            Instant current = inputWatermarks.get(i);
            if (current.isBefore(min)) {
                min = current;
            }
        }
        LOG.debug("event=watermark_propagate_union inputs={} result={}", inputWatermarks.size(), min);
        return min;
    }

    /**
     * Computes the output watermark for a join operator.
     * Join semantics: min of left and right watermarks.
     *
     * @param leftWatermark  watermark from the left input
     * @param rightWatermark watermark from the right input
     * @return the minimum of the two watermarks
     */
    public static Instant propagateJoin(Instant leftWatermark, Instant rightWatermark) {
        Objects.requireNonNull(leftWatermark, "leftWatermark must not be null");
        Objects.requireNonNull(rightWatermark, "rightWatermark must not be null");
        Instant result = leftWatermark.isBefore(rightWatermark) ? leftWatermark : rightWatermark;
        LOG.debug("event=watermark_propagate_join left={} right={} result={}", leftWatermark, rightWatermark, result);
        return result;
    }

    /**
     * Computes the output watermark for an aggregation operator.
     * Aggregation semantics: input watermark minus allowed lateness.
     *
     * @param inputWatermark   watermark from the aggregation input
     * @param allowedLateness  how long to hold back the watermark for late data
     * @return the adjusted watermark
     */
    public static Instant propagateAggregation(Instant inputWatermark, Duration allowedLateness) {
        Objects.requireNonNull(inputWatermark, "inputWatermark must not be null");
        Objects.requireNonNull(allowedLateness, "allowedLateness must not be null");
        Instant result = inputWatermark.minus(allowedLateness);
        LOG.debug("event=watermark_propagate_aggregation input={} lateness={} result={}",
                inputWatermark, allowedLateness, result);
        return result;
    }

    /**
     * Pass-through propagation for filter and project operators.
     * These operators do not affect event time semantics.
     *
     * @param inputWatermark watermark from the input
     * @return the same watermark unchanged
     */
    public static Instant propagatePassThrough(Instant inputWatermark) {
        Objects.requireNonNull(inputWatermark, "inputWatermark must not be null");
        return inputWatermark;
    }
}
