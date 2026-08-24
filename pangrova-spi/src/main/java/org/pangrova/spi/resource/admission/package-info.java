/**
 * SPI for cluster-level query admission control.
 *
 * <p>Pangrova's existing admission control (via {@code QuotaChecker}) was shallow,
 * had a check-then-act race, and operated only on a single coordinator's local
 * counters with no cluster-wide enforcement (G-RES-04). This package defines
 * the SPI for a proper cluster-level admission controller.</p>
 *
 * <h3>Key types</h3>
 * <ul>
 *   <li>{@link org.pangrova.spi.resource.admission.ClusterAdmissionController} --
 *       main SPI: atomic admit/queue/reject with cluster-wide visibility</li>
 *   <li>{@link org.pangrova.spi.resource.admission.AdmissionRequest} --
 *       request context (tenant, resource group, query id, memory estimate)</li>
 *   <li>{@link org.pangrova.spi.resource.admission.AdmissionDecision} --
 *       decision output (verdict + reason)</li>
 *   <li>{@link org.pangrova.spi.resource.admission.AdmissionVerdict} --
 *       enum: ADMIT, QUEUE, REJECT</li>
 * </ul>
 *
 * <p>Benchmark: Trino resource groups (tiered concurrency + queue limits),
 * Snowflake multi-cluster warehouse admission, Databricks SQL admission control.</p>
 */
package org.pangrova.spi.resource.admission;
