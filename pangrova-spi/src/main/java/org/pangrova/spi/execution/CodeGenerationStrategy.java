package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Experimental;

/**
 * Strategy controlling how expressions and aggregations are evaluated at runtime.
 *
 * <p>Today Pangrova delegates all expression evaluation to the native vectorized
 * execution engine, which interprets expression trees over columnar batches. This works
 * well for simple projections and filters but leaves performance on the table
 * for complex expressions, deeply nested CASE/WHEN chains, and aggregation
 * combiners where compiled code (as in Spark Photon, DuckDB, or DataFusion)
 * can yield 2-5x speedups.</p>
 *
 * <p>This enum is an <em>extension point</em>: the engine reads the configured
 * strategy and passes it through the plan context so that future backends can
 * register a JIT or AOT code-generation provider without modifying the core
 * scheduling path.</p>
 *
 * <h3>Integration with the native engine</h3>
 * <p>The native engine has an experimental expression-compilation path
 * ({@code NativeExpressionCompiler}). When that stabilises, it can be surfaced
 * as a {@link #JIT_COMPILED} strategy implementation behind the JNI bridge.</p>
 *
 * <p>Configuration key: {@code execution.codegen-strategy}</p>
 *
 * @see ExecutionCapabilities
 */
@Experimental(reason = "G-EXEC-05: extension point for future JIT/AOT expression compilation backends")
public enum CodeGenerationStrategy {

    /**
     * Interpret expression trees over columnar batches (current native engine default).
     *
     * <p>No runtime code generation. Expression evaluation walks the tree
     * node-by-node, applying vectorized primitives per batch. This is the
     * safest and most portable mode.</p>
     */
    INTERPRETED,

    /**
     * Compile expressions into native code at query execution time using a
     * just-in-time compilation backend, then invoke the compiled function
     * for subsequent batches within the same query.
     *
     * <p>Not yet implemented. When a backend registers for this strategy,
     * the engine will call its compilation API during plan lowering and
     * attach the compiled artifact to the Substrait plan fragment.
     * JIT compilation amortises over long-running queries but adds
     * first-batch latency.</p>
     */
    JIT_COMPILED,

    /**
     * Pre-compile expressions ahead of time during query planning, caching
     * compiled artifacts for reuse across queries with identical expression
     * shapes.
     *
     * <p>Not yet implemented. AOT compilation eliminates the first-batch
     * latency penalty of JIT but requires a compilation cache and a
     * deterministic expression fingerprinting scheme. This is the strategy
     * used by systems like Databricks Photon for frequently executed
     * expression patterns.</p>
     */
    AOT_COMPILED
}
