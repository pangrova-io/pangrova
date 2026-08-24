/**
 * SPI for per-query runaway detection and noisy-neighbour defence.
 *
 * <p>Pangrova previously had no per-query big-query guard and no memory-based
 * query killer (G-RES-06). The reactive {@code CriticalPressureHandler}
 * kills the largest consumer only once the node is already in CRITICAL
 * pressure; this package defines the SPI for a <em>proactive</em> per-query
 * guard that detects runaways before the node reaches crisis.</p>
 *
 * <h3>Key types</h3>
 * <ul>
 *   <li>{@link org.pangrova.spi.resource.guard.RunawayQueryGuard} --
 *       main SPI: evaluates queries against resource thresholds</li>
 *   <li>{@link org.pangrova.spi.resource.guard.QueryResourceSnapshot} --
 *       point-in-time resource consumption of a single query</li>
 *   <li>{@link org.pangrova.spi.resource.guard.GuardVerdict} --
 *       verdict output (query id + action + breach reasons)</li>
 *   <li>{@link org.pangrova.spi.resource.guard.GuardAction} --
 *       enum: KILL, THROTTLE, WARN</li>
 * </ul>
 *
 * <p>Benchmark: StarRocks big-query log / auto-cancel, Trino low-memory killer,
 * Snowflake query timeout / resource monitor.</p>
 */
package org.pangrova.spi.resource.guard;
