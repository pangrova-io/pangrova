/**
 * Feature store SPI for point-in-time feature engineering.
 *
 * <p>This package defines the service provider interface for feature stores
 * that support ML training and inference workflows within the Pangrova query
 * engine. The primary use case is point-in-time feature lookups that prevent
 * data leakage during model training.</p>
 *
 * <p>The key interfaces and types:</p>
 * <ul>
 *   <li>{@link org.pangrova.spi.indexing.feature.FeatureStore} — the main SPI
 *       contract for feature retrieval</li>
 *   <li>{@link org.pangrova.spi.indexing.feature.FeatureGroup} — descriptor for
 *       a logical group of related features</li>
 *   <li>{@link org.pangrova.spi.indexing.feature.FeatureValue} — a point-in-time
 *       snapshot of feature values for an entity</li>
 * </ul>
 *
 * @see org.pangrova.spi.indexing.feature.FeatureStore
 */
package org.pangrova.spi.indexing.feature;
