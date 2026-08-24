/**
 * SPI for cluster auto-scaling, scale-in, and auto-suspend decisions.
 *
 * <p>Pangrova previously had only a fixed worker pool with no elastic behaviour
 * (G-RES-03). This package defines the SPI for pluggable auto-scaling
 * strategies and scaling executors.</p>
 *
 * <h3>Key types</h3>
 * <ul>
 *   <li>{@link org.pangrova.spi.resource.scaling.AutoScaler} --
 *       main SPI: evaluates cluster metrics, returns a scaling decision</li>
 *   <li>{@link org.pangrova.spi.resource.scaling.ScalingExecutor} --
 *       applies scaling decisions to infrastructure (K8s, cloud, manual)</li>
 *   <li>{@link org.pangrova.spi.resource.scaling.ClusterMetricsSnapshot} --
 *       point-in-time cluster metrics input</li>
 *   <li>{@link org.pangrova.spi.resource.scaling.ScalingDecision} --
 *       decision output (direction + target + reason)</li>
 *   <li>{@link org.pangrova.spi.resource.scaling.ScalingDirection} --
 *       enum: SCALE_OUT, SCALE_IN, SUSPEND, HOLD</li>
 *   <li>{@link org.pangrova.spi.resource.scaling.ShuffleDataResidencyCheck} --
 *       checks shuffle data residency before scale-down (G-RES-08)</li>
 * </ul>
 *
 * <p>Benchmark: Snowflake warehouse auto-suspend, Databricks SQL auto-stop,
 * Trino resource group soft limits, Spark ESS dynamic allocation.</p>
 */
package org.pangrova.spi.resource.scaling;
