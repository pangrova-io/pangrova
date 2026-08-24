package org.pangrova.spi.indexing.feature;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;

/**
 * Descriptor for a feature group in the feature store.
 *
 * <p>A feature group is a logical collection of related features computed
 * from the same source entity. For example, a "user_profile" feature group
 * might contain features like age, location, and signup_date.</p>
 *
 * @param name            unique name of the feature group (must not be blank)
 * @param description     human-readable description (may be empty, never null)
 * @param entityIdColumn  the column name used as the entity identifier
 * @param featureNames    ordered list of feature names in this group
 * @param timestampColumn the column used for point-in-time versioning;
 *                        null if the group does not support time-travel
 */
@Experimental(reason = "G-AI-08: feature store SPI for ML training")
public record FeatureGroup(
        String name,
        String description,
        String entityIdColumn,
        List<String> featureNames,
        String timestampColumn) {

    public FeatureGroup {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be null or blank");
        }
        if (description == null) {
            description = "";
        }
        if (entityIdColumn == null || entityIdColumn.isBlank()) {
            throw new IllegalArgumentException("entityIdColumn must not be null or blank");
        }
        if (featureNames == null || featureNames.isEmpty()) {
            throw new IllegalArgumentException("featureNames must not be null or empty");
        }
        featureNames = List.copyOf(featureNames);
    }
}
