/**
 * SPI for rule-based query routing and workload classification.
 *
 * <p>Pangrova previously had no selector/classifier layer to map inbound queries
 * to resource groups (G-RES-05). This package defines the SPI for pluggable
 * workload classifiers that route queries by user, source, client tags,
 * query type, database, source IP, and (future) plan cost.</p>
 *
 * <h3>Key types</h3>
 * <ul>
 *   <li>{@link org.pangrova.spi.resource.classification.WorkloadClassifier} --
 *       main SPI: classifies query attributes into a resource group</li>
 *   <li>{@link org.pangrova.spi.resource.classification.QueryAttributes} --
 *       inbound query attributes for classification</li>
 *   <li>{@link org.pangrova.spi.resource.classification.WorkloadClass} --
 *       classification result (resource group id + priority)</li>
 * </ul>
 *
 * <p>Benchmark: Trino resource group selectors (user/source/clientTags/queryType
 * rules, first-match-wins, DB-backed hot reload).</p>
 */
package org.pangrova.spi.resource.classification;
