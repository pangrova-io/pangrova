/**
 * User-defined function (UDF) SPI for the Pangrova execution engine.
 *
 * <p>This package defines the contract for registering and executing
 * user-defined scalar functions. Functions can run in multiple execution
 * modes — from high-isolation external processes to zero-overhead native
 * vectorized evaluation inside the native engine.</p>
 *
 * <h3>Key types</h3>
 * <ul>
 *   <li>{@link org.pangrova.spi.execution.udf.UserDefinedFunction} —
 *       the primary SPI interface that function authors implement.</li>
 *   <li>{@link org.pangrova.spi.execution.udf.UdfExecutionMode} —
 *       enum declaring where/how a UDF executes.</li>
 * </ul>
 *
 * <h3>Status</h3>
 * <p>This SPI is {@link org.pangrova.spi.annotation.Experimental @Experimental}.
 * The evaluate signature is expected to evolve toward columnar batch input
 * (Arrow vectors) in a future release to enable true vectorized Java UDFs.</p>
 *
 * @see org.pangrova.spi.execution.udf.UserDefinedFunction
 * @see org.pangrova.spi.execution.udf.UdfExecutionMode
 */
package org.pangrova.spi.execution.udf;
