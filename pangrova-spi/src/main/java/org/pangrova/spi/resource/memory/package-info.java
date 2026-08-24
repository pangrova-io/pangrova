/**
 * SPI for unified memory budget enforcement across Java, native, and off-heap domains.
 *
 * <p>Pangrova's Java and native arbitrators were not truly unified: the shared
 * path was off by default and dynamic levers (shrink, spill/reclaim) were inert
 * (G-RES-07). This package defines the SPI for a cross-domain memory budget
 * enforcer that decides spill/shrink/kill actions based on merged utilisation.</p>
 *
 * <h3>Key types</h3>
 * <ul>
 *   <li>{@link org.pangrova.spi.resource.memory.MemoryBudgetEnforcer} --
 *       main SPI: evaluates cross-domain snapshots, returns enforcement decisions</li>
 *   <li>{@link org.pangrova.spi.resource.memory.MemoryDomainSnapshot} --
 *       per-domain usage snapshot (domain, used bytes, budget bytes)</li>
 *   <li>{@link org.pangrova.spi.resource.memory.MemoryDomain} --
 *       enum: JAVA_HEAP, NATIVE, OFF_HEAP</li>
 *   <li>{@link org.pangrova.spi.resource.memory.EnforcementDecision} --
 *       decision output (action + target domain + reason)</li>
 *   <li>{@link org.pangrova.spi.resource.memory.EnforcementAction} --
 *       enum: NONE, SPILL, SHRINK_NATIVE, RECLAIM, KILL_LARGEST</li>
 * </ul>
 *
 * <p>Benchmark: Spark unified memory manager, native engine shared memory arbitrator,
 * Trino memory pool + low-memory killer, Presto memory revocation.</p>
 */
package org.pangrova.spi.resource.memory;
