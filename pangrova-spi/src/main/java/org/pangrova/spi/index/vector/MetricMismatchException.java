package org.pangrova.spi.index.vector;

/**
 * Thrown when the distance metric specified in a search request does not
 * match the metric used to build the vector index.
 *
 * <p>For example, searching with {@link DistanceMetric#COSINE} against an
 * index built with {@link DistanceMetric#L2} is invalid and will produce
 * this exception.</p>
 */
public class MetricMismatchException extends RuntimeException {

    public MetricMismatchException(String message) {
        super(message);
    }

    public MetricMismatchException(DistanceMetric searchMetric, DistanceMetric buildMetric) {
        super("Search metric " + searchMetric + " does not match build metric " + buildMetric);
    }
}
