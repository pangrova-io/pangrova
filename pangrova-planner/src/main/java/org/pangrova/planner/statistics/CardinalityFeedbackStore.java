package org.pangrova.planner.statistics;

import org.pangrova.planner.api.statistics.CardinalityFeedback;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

/**
 * History-based cardinality feedback keyed by query shape (e.g. a plan-node or query
 * fingerprint). Records the {@code actual/estimated} cardinality ratio observed at runtime and
 * exponentially smooths it into a per-shape correction factor, so that repeated query shapes
 * have their estimates progressively corrected toward observed reality — the engine-agnostic
 * mechanism behind history-based optimisation (cf. Oracle SQL Plan Directives, Microsoft
 * cardinality feedback, Trino history-based stats).
 *
 * <p>For a stable shape whose estimate and actual stay constant, the smoothed factor converges
 * geometrically to {@code actual/estimated}, so {@link #correctedEstimate} converges to the
 * true cardinality.</p>
 *
 * <p>This in-memory store is the decision core; durable cross-restart persistence (a JDBC/KV
 * backend) is a separate concern that can wrap or feed this store.</p>
 */
@ThreadSafe
public final class CardinalityFeedbackStore {

    /** Default smoothing weight on the newest observation. */
    public static final double DEFAULT_ALPHA = 0.3;

    private final double alpha;
    private final ConcurrentHashMap<String, CardinalityFeedback> entries = new ConcurrentHashMap<>();

    public CardinalityFeedbackStore() {
        this(DEFAULT_ALPHA);
    }

    /**
     * Creates a feedback store with the given exponential smoothing factor.
     *
     * @param alpha smoothing factor in (0.0, 1.0] applied to observed cardinalities
     */
    public CardinalityFeedbackStore(double alpha) {
        if (alpha <= 0.0 || alpha > 1.0) {
            throw new IllegalArgumentException("alpha must be in (0.0, 1.0], got " + alpha);
        }
        this.alpha = alpha;
    }

    /**
     * Folds one runtime observation into the shape's correction factor. Non-positive row counts
     * are ignored (no signal). The first observation seeds the factor with the raw ratio.
     *
     * @param shapeKey      stable identifier of the query shape; must not be null/blank
     * @param estimatedRows the planner's estimate for this shape
     * @param actualRows    the row count actually observed at runtime
     */
    public void record(String shapeKey, long estimatedRows, long actualRows) {
        requireKey(shapeKey);
        if (estimatedRows <= 0 || actualRows <= 0) {
            return;
        }
        double ratio = (double) actualRows / estimatedRows;
        entries.compute(shapeKey, (k, existing) -> {
            if (existing == null) {
                return new CardinalityFeedback(ratio, 1);
            }
            double smoothed = alpha * ratio + (1.0 - alpha) * existing.correctionFactor();
            return new CardinalityFeedback(smoothed, existing.observationCount() + 1);
        });
    }

    /** @return the learned correction factor for the shape, if any observations exist. */
    public OptionalDouble correctionFactor(String shapeKey) {
        requireKey(shapeKey);
        CardinalityFeedback fb = entries.get(shapeKey);
        return fb == null ? OptionalDouble.empty() : OptionalDouble.of(fb.correctionFactor());
    }

    /** @return the full feedback summary for the shape, if any observations exist. */
    public Optional<CardinalityFeedback> get(String shapeKey) {
        requireKey(shapeKey);
        return Optional.ofNullable(entries.get(shapeKey));
    }

    /**
     * Corrects a fresh planner estimate using accumulated history for the shape. Returns the
     * estimate unchanged when no history exists, so callers can apply this unconditionally.
     *
     * @param shapeKey      the query shape
     * @param freshEstimate the planner's current estimate
     * @return the history-corrected estimate ({@code freshEstimate × correctionFactor})
     */
    public double correctedEstimate(String shapeKey, double freshEstimate) {
        OptionalDouble factor = correctionFactor(shapeKey);
        return factor.isPresent() ? freshEstimate * factor.getAsDouble() : freshEstimate;
    }

    public int size() {
        return entries.size();
    }

    public void clear() {
        entries.clear();
    }

    private static void requireKey(String shapeKey) {
        Objects.requireNonNull(shapeKey, "shapeKey must not be null");
        if (shapeKey.isBlank()) {
            throw new IllegalArgumentException("shapeKey must not be blank");
        }
    }
}
