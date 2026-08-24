package org.pangrova.operations.scaling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Predictive scaling that acts only when prediction confidence exceeds threshold.
 */
public final class PredictiveScaling {

    private static final Logger LOG = LoggerFactory.getLogger(PredictiveScaling.class);
    private static final double DEFAULT_CONFIDENCE_THRESHOLD = 0.8;

    private final Predictor predictor;
    private final double confidenceThreshold;

    public PredictiveScaling(Predictor predictor) {
        this(predictor, DEFAULT_CONFIDENCE_THRESHOLD);
    }

    public PredictiveScaling(Predictor predictor, double confidenceThreshold) {
        this.predictor = Objects.requireNonNull(predictor, "predictor");
        this.confidenceThreshold = confidenceThreshold;
    }

    public int evaluate(List<MetricSnapshot> history, int currentWorkers) {
        Prediction prediction = predictor.predict(history);
        if (prediction.confidence() < confidenceThreshold) {
            LOG.debug("Prediction confidence {} below threshold {}", prediction.confidence(), confidenceThreshold);
            return currentWorkers;
        }
        return deriveTarget(prediction.score(), currentWorkers);
    }

    private int deriveTarget(double score, int currentWorkers) {
        if (score > 0.8) {
            return currentWorkers + 2;
        }
        if (score > 0.6) {
            return currentWorkers + 1;
        }
        return currentWorkers;
    }

    /**
     * Predicts future load from historical metrics.
     */
    public interface Predictor {
        Prediction predict(List<MetricSnapshot> history);
    }

    /**
     * Prediction result with score and confidence level.
     */
    public record Prediction(double score, double confidence) {
    }
}
