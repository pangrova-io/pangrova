package org.pangrova.spi.execution.udf;

import java.util.List;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.data.type.Type;

/**
 * SPI for registering and invoking user-defined scalar functions (UDFs).
 *
 * <p>Implementations represent a single named scalar function that can be
 * called from SQL. The engine discovers registered functions via service
 * loading or catalog metadata and invokes them during expression evaluation.</p>
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>The engine calls {@link #name()} to register the function in the
 *       function namespace.</li>
 *   <li>During planning, {@link #returnType(List)} is called with the
 *       concrete argument types to resolve the output type (supporting
 *       overloaded / polymorphic functions).</li>
 *   <li>At execution time, {@link #evaluate(Object...)} is called once per
 *       row (for {@link UdfExecutionMode#IN_ENGINE_JAVA}) or once per batch
 *       (for external/native modes, with appropriate batching wrappers).</li>
 * </ol>
 *
 * <h3>Thread safety</h3>
 * <p>Implementations <em>must</em> be thread-safe. A single instance may be
 * shared across concurrent tasks on the same worker. If per-invocation state
 * is needed, use {@link ThreadLocal} or accept a context parameter.</p>
 *
 * <h3>Execution mode</h3>
 * <p>The {@link #executionMode()} method declares how the engine should
 * dispatch this function. The default is {@link UdfExecutionMode#IN_ENGINE_JAVA}.
 * Functions that declare {@link UdfExecutionMode#SCALAR_SQL} must override
 * {@link #sqlBody()} to provide the inlineable SQL expression.</p>
 *
 * <h3>Example</h3>
 * <pre>{@code
 * public class DoubleItFunction implements UserDefinedFunction {
 *     public String name() { return "double_it"; }
 *
 *     @Override
 *     public Type returnType(List<Type> argumentTypes) {
 *         return argumentTypes.get(0); // same type as input
 *     }
 *
 *     @Override
 *     public Object evaluate(Object... args) {
 *         if (args[0] == null) return null;
 *         return ((Number) args[0]).longValue() * 2;
 *     }
 * }
 * }</pre>
 *
 * @see UdfExecutionMode
 */
@Experimental(reason = "G-EXEC-07: first-pass UDF SPI; evaluate signature will likely evolve to columnar batches")
public interface UserDefinedFunction {

    /**
     * Returns the SQL-visible name of this function.
     *
     * <p>Names are case-insensitive and must be unique within a catalog
     * schema. The engine normalises names to lower-case during registration.</p>
     *
     * @return the function name, never null or empty
     */
    String name();

    /**
     * Resolves the return type given the concrete argument types.
     *
     * <p>Called at planning time. Implementations should validate argument
     * count and types, throwing {@link IllegalArgumentException} if the
     * arguments are invalid for this function.</p>
     *
     * @param argumentTypes the concrete types of the call-site arguments,
     *                      never null, may be empty for nullary functions
     * @return the resolved return type, never null
     * @throws IllegalArgumentException if the argument types are invalid
     */
    Type returnType(List<Type> argumentTypes);

    /**
     * Evaluates the function for a single row.
     *
     * <p>Arguments are passed as boxed Java objects matching the Pangrova type
     * mapping (Long for BIGINT, Double for DOUBLE, String for VARCHAR, etc.).
     * {@code null} arguments are passed as Java {@code null}; implementations
     * must handle nulls explicitly.</p>
     *
     * <p>For {@link UdfExecutionMode#IN_ENGINE_JAVA} functions this is the
     * primary execution entry point. For other modes this method may serve as
     * a fallback or testing hook.</p>
     *
     * @param args the function arguments, positionally ordered
     * @return the function result, or null for SQL NULL
     */
    Object evaluate(Object... args);

    /**
     * Returns the execution mode for this function.
     *
     * <p>Defaults to {@link UdfExecutionMode#IN_ENGINE_JAVA}. Override to
     * declare a different dispatch strategy.</p>
     *
     * @return the execution mode, never null
     */
    default UdfExecutionMode executionMode() {
        return UdfExecutionMode.IN_ENGINE_JAVA;
    }

    /**
     * Returns the SQL expression body for {@link UdfExecutionMode#SCALAR_SQL}
     * functions.
     *
     * <p>The body is a SQL expression using positional parameter references
     * ({@code $1}, {@code $2}, ...) that the planner inlines at the call
     * site. Example: {@code "$1 * 2 + $2"}.</p>
     *
     * <p>Only meaningful when {@link #executionMode()} returns
     * {@link UdfExecutionMode#SCALAR_SQL}. Default returns {@code null}.</p>
     *
     * @return the SQL expression body, or null if not a SQL-defined function
     */
    default String sqlBody() {
        return null;
    }

    /**
     * Returns a human-readable description of this function, suitable for
     * SHOW FUNCTIONS output and documentation generation.
     *
     * @return description text, or empty string if none
     */
    default String description() {
        return "";
    }
}
