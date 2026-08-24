package org.pangrova.spi.execution.udf;

import org.pangrova.spi.annotation.Experimental;

/**
 * Describes where and how a user-defined function (UDF) executes within the
 * Pangrova engine.
 *
 * <h3>Problem statement (G-EXEC-07)</h3>
 * <p>The current UDF layering is incomplete: all user functions run in a
 * separate process (EXTERNAL_PROCESS) with high per-row serialisation
 * overhead. There is no in-engine Java scalar path, no SQL-defined scalar
 * function path, and no way to register a native vectorized UDF that runs
 * inside the native vectorized evaluation loop.</p>
 *
 * <h3>Execution modes</h3>
 * <ul>
 *   <li>{@link #EXTERNAL_PROCESS} — current default; UDF runs in a
 *       subprocess or sidecar, data crosses process boundaries per batch.</li>
 *   <li>{@link #IN_ENGINE_JAVA} — UDF class is loaded into the worker JVM
 *       and invoked directly in the Java task thread, avoiding IPC overhead.</li>
 *   <li>{@link #SCALAR_SQL} — function body is a SQL expression that is
 *       inlined into the query plan at optimisation time.</li>
 *   <li>{@link #NATIVE_VECTORIZED} — UDF is compiled to native code and
 *       registered as a Velox simple-function, evaluated inside the native
 *       vectorized loop with zero JNI crossings per row.</li>
 * </ul>
 *
 * <p>The mode is declared per function via
 * {@link UserDefinedFunction#executionMode()}.</p>
 *
 * @see UserDefinedFunction
 */
@Experimental(reason = "G-EXEC-07: extension point for pluggable UDF execution strategies")
public enum UdfExecutionMode {

    /**
     * UDF runs in a separate OS process or container.
     *
     * <p>Data is serialised (columnar IPC or JSON) across the process boundary
     * for each batch. Highest isolation but highest per-row overhead.</p>
     */
    EXTERNAL_PROCESS,

    /**
     * UDF class is loaded into the worker JVM and invoked inline.
     *
     * <p>The function's {@link UserDefinedFunction#evaluate} method is called
     * directly from the Java task thread, eliminating IPC overhead. The
     * function shares the worker's memory space and must be thread-safe.</p>
     */
    IN_ENGINE_JAVA,

    /**
     * Function body is a SQL expression that the planner inlines.
     *
     * <p>At plan time the SQL body is parsed, type-checked, and spliced into
     * the calling expression tree. No runtime dispatch — the function
     * effectively becomes a macro. Best for simple derived columns.</p>
     */
    SCALAR_SQL,

    /**
     * UDF is compiled to a native simple-function or vector-function.
     *
     * <p>The function runs entirely inside the native vectorized evaluation
     * loop with zero JNI crossings per row. Requires a native shared library
     * that registers the function with the native engine at worker startup.</p>
     */
    NATIVE_VECTORIZED
}
