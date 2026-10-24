/**
 * Connector-facing contracts for the unified materialization kernel.
 *
 * <p>These types are intentionally defined in {@code pangrova-spi} rather than
 * in {@code pangrova-materialization}: connectors may only depend on
 * {@code spi}, {@code common}, and {@code protocol}, yet they must implement
 * source snapshot, change-set, and commit-evidence contracts. The kernel
 * depends on this package; the dependency never points the other way.
 */
package org.pangrova.spi.materialization;
