package org.pangrova.spi.indexing.feature;

import org.pangrova.spi.annotation.Experimental;

import java.time.Instant;
import java.util.Map;

/**
 * A snapshot of feature values for a single entity at a point in time.
 *
 * <p>Each entry in the {@code values} map represents one feature dimension,
 * keyed by feature name. The value type is {@link Object} to accommodate
 * heterogeneous feature types (numeric, string, boolean, array, etc.).
 * Type safety is enforced by the feature group schema at registration time,
 * not at lookup time.</p>
 *
 * @param entityId      the entity identifier this value belongs to
 * @param featureGroup  the feature group name
 * @param values        map of feature name to feature value (defensively copied)
 * @param eventTime     the event timestamp of this feature snapshot (when the
 *                      feature was computed or observed)
 */
@Experimental(reason = "G-AI-08: feature store SPI for ML training")
public record FeatureValue(
        String entityId,
        String featureGroup,
        Map<String, Object> values,
        Instant eventTime) {

    public FeatureValue {
        if (entityId == null || entityId.isBlank()) {
            throw new IllegalArgumentException("entityId must not be null or blank");
        }
        if (featureGroup == null || featureGroup.isBlank()) {
            throw new IllegalArgumentException("featureGroup must not be null or blank");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }
        values = Map.copyOf(values);
        if (eventTime == null) {
            throw new IllegalArgumentException("eventTime must not be null");
        }
    }
}
