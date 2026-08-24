package org.pangrova.spi.indexing.feature;

import org.pangrova.spi.annotation.Experimental;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * SPI contract for a feature store providing point-in-time feature lookups
 * for ML training and inference pipelines.
 *
 * <p>A feature store decouples feature engineering from model training by
 * providing a consistent, versioned view of features at any historical
 * timestamp. This enables:</p>
 * <ul>
 *   <li><strong>Point-in-time correctness:</strong> training data reflects
 *       exactly the features that were available at each label timestamp,
 *       preventing data leakage from future values</li>
 *   <li><strong>Feature reuse:</strong> features computed once can be shared
 *       across multiple models and teams</li>
 *   <li><strong>Online/offline consistency:</strong> the same feature
 *       definitions serve both batch training and real-time inference</li>
 * </ul>
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}.
 * Implementations must be thread-safe.</p>
 *
 * <p>Industry references:
 * <ul>
 *   <li>Feast: open-source feature store with offline/online stores</li>
 *   <li>Tecton: managed feature platform with point-in-time joins</li>
 *   <li>Databricks Feature Store: Unity Catalog-integrated feature tables</li>
 *   <li>SageMaker Feature Store: AWS-managed online/offline store</li>
 *   <li>Hopsworks: open-source feature store with time-travel</li>
 * </ul>
 * </p>
 *
 * @see FeatureGroup
 * @see FeatureValue
 */
@Experimental(reason = "G-AI-08: feature store SPI for ML training")
public interface FeatureStore {

    /**
     * Retrieves features for a single entity at a specific point in time.
     *
     * <p>The returned feature values reflect the state of the feature group
     * as of {@code asOfTimestamp}. This guarantees point-in-time correctness:
     * only features that were known at the given timestamp are returned,
     * preventing data leakage from future feature values.</p>
     *
     * <p>If no features exist for the entity at the given timestamp, returns
     * {@link Optional#empty()}.</p>
     *
     * @param entityId      the entity identifier (e.g., user ID, product ID)
     * @param featureGroup  the feature group to query
     * @param asOfTimestamp the point-in-time for the feature lookup
     * @return the feature values at the given timestamp, or empty if not found
     * @throws IllegalArgumentException if any parameter is null or entityId is blank
     */
    Optional<FeatureValue> getFeatures(String entityId, String featureGroup,
                                       Instant asOfTimestamp);

    /**
     * Retrieves features for multiple entities at a specific point in time
     * (batch lookup).
     *
     * <p>This is the batch equivalent of {@link #getFeatures(String, String, Instant)}.
     * Implementations should optimize for throughput (e.g., batched I/O).</p>
     *
     * <p>The returned map contains an entry for each entity ID that has
     * features at the given timestamp. Entity IDs with no features are
     * absent from the map.</p>
     *
     * @param entityIds     the entity identifiers to look up
     * @param featureGroup  the feature group to query
     * @param asOfTimestamp the point-in-time for the feature lookup
     * @return map from entity ID to feature values; missing entities are omitted
     * @throws IllegalArgumentException if any parameter is null or entityIds is empty
     */
    Map<String, FeatureValue> getFeaturesBatch(List<String> entityIds,
                                                String featureGroup,
                                                Instant asOfTimestamp);

    /**
     * Lists available feature groups in this store.
     *
     * @return list of feature group descriptors, never null
     */
    List<FeatureGroup> listFeatureGroups();

    /**
     * Returns the name of this feature store implementation.
     *
     * @return the store name (never null or blank)
     */
    String storeName();
}
