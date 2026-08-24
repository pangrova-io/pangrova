package org.pangrova.worker.jni;

import org.pangrova.spi.annotation.ThreadSafe;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.AggregateFunction;
import org.pangrova.spi.function.BatchableScalarFunction;
import org.pangrova.spi.function.FunctionMetadata;
import org.pangrova.spi.function.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Bridge between Java UDF/UDAF implementations (from the function SPI) and
 * the native execution engine.
 *
 * <p>This bridge has two responsibilities:
 * <ol>
 *   <li><b>Registration:</b> At worker startup, it collects function descriptors
 *       from {@link org.pangrova.spi.function.FunctionFactory} instances and
 *       registers them with the native engine via JNI. The native side creates
 *       proxy functions that route evaluation back to Java.</li>
 *   <li><b>Evaluation callback:</b> When the native engine encounters a registered
 *       UDF during query execution, it calls back into this bridge via JNI
 *       ({@link #evaluateScalar}, {@link #createAccumulator},
 *       {@link #addInput}, {@link #getAggregateResult}). The bridge
 *       dispatches to the corresponding Java function implementation.</li>
 * </ol>
 *
 * <p>Thread safety: this class is fully thread-safe. The function registries
 * use {@link ConcurrentHashMap}, and individual function implementations are
 * required to be thread-safe by the SPI contract.</p>
 *
 * <p>Design note: function names are stored in lower-case for case-insensitive
 * matching, consistent with SQL identifier semantics.</p>
 *
 * @see ScalarFunction
 * @see AggregateFunction
 * @see NativeFunctionDescriptor
 */
@ThreadSafe
public final class NativeFunctionBridge {

    private static final Logger LOG = LoggerFactory.getLogger(NativeFunctionBridge.class);

    private final ConcurrentHashMap<String, ScalarFunction> scalarFunctions =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AggregateFunction> aggregateFunctions =
            new ConcurrentHashMap<>();

    /**
     * Registers a scalar function for native callback evaluation.
     *
     * @param function the scalar function to register
     * @throws NullPointerException     if function or its metadata is null
     * @throws IllegalArgumentException if a function with the same name is already registered
     */
    public void registerScalar(ScalarFunction function) {
        checkNotNull(function, "function must not be null");
        FunctionMetadata meta = checkNotNull(function.getMetadata(), "function metadata must not be null");
        String key = meta.name().toLowerCase(java.util.Locale.ROOT);
        ScalarFunction existing = scalarFunctions.putIfAbsent(key, function);
        if (existing != null) {
            throw new IllegalArgumentException(
                    "Scalar function already registered: " + meta.name());
        }
        LOG.info("event=scalar_function_registered name={} returnType={} argTypes={}",
                meta.name(), meta.returnType(), meta.argumentTypes());
    }

    /**
     * Registers an aggregate function for native callback evaluation.
     *
     * @param function the aggregate function to register
     * @throws NullPointerException     if function or its metadata is null
     * @throws IllegalArgumentException if a function with the same name is already registered
     */
    public void registerAggregate(AggregateFunction function) {
        checkNotNull(function, "function must not be null");
        FunctionMetadata meta = checkNotNull(function.getMetadata(), "function metadata must not be null");
        String key = meta.name().toLowerCase(java.util.Locale.ROOT);
        AggregateFunction existing = aggregateFunctions.putIfAbsent(key, function);
        if (existing != null) {
            throw new IllegalArgumentException(
                    "Aggregate function already registered: " + meta.name());
        }
        LOG.info("event=aggregate_function_registered name={} returnType={} argTypes={}",
                meta.name(), meta.returnType(), meta.argumentTypes());
    }

    /**
     * Builds descriptors for all registered functions, suitable for sending
     * to the native engine during startup.
     *
     * @return unmodifiable list of descriptors for all registered functions
     */
    public List<NativeFunctionDescriptor> buildDescriptors() {
        var descriptors = new java.util.ArrayList<NativeFunctionDescriptor>();
        for (ScalarFunction fn : scalarFunctions.values()) {
            FunctionMetadata m = fn.getMetadata();
            descriptors.add(new NativeFunctionDescriptor(
                    m.name(), m.returnType(), m.argumentTypes(),
                    NativeFunctionDescriptor.FunctionType.SCALAR, m.deterministic()));
        }
        for (AggregateFunction fn : aggregateFunctions.values()) {
            FunctionMetadata m = fn.getMetadata();
            descriptors.add(new NativeFunctionDescriptor(
                    m.name(), m.returnType(), m.argumentTypes(),
                    NativeFunctionDescriptor.FunctionType.AGGREGATE, m.deterministic()));
        }
        return Collections.unmodifiableList(descriptors);
    }

    // ---- JNI callback methods (called from native proxy functions) ----

    /**
     * Evaluates a batch of rows for a registered scalar function.
     *
     * <p>Called from native code when the native engine has accumulated multiple rows for a UDF.
     * If the function implements {@link BatchableScalarFunction}, uses the batch path;
     * otherwise falls back to row-by-row invocation.</p>
     *
     * @param functionName   the function name (case-insensitive)
     * @param batchArguments array of per-row argument arrays
     * @return array of results, one per input row
     */
    public Object[] evaluateBatch(String functionName, Object[][] batchArguments) {
        String key = functionName.toLowerCase(Locale.ROOT);
        ScalarFunction fn = scalarFunctions.get(key);
        if (fn == null) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Scalar function not registered: " + functionName);
        }
        try {
            if (fn instanceof BatchableScalarFunction batchable) {
                List<List<ColumnValue>> batch = Arrays.stream(batchArguments)
                        .map(NativeFunctionBridge::wrapArguments)
                        .toList();
                List<ColumnValue> results = batchable.invokeBatch(batch);
                return results.stream()
                        .map(ColumnValue::toRaw)
                        .toArray();
            }
            // Fallback: row-by-row
            return Arrays.stream(batchArguments)
                    .map(args -> evaluateScalar(functionName, args))
                    .toArray();
        } catch (NativeRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Batch evaluation of '" + functionName + "' failed: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Evaluates a registered scalar function.
     *
     * <p>Called from native code via JNI when the native engine executes a UDF proxy.
     * The native side serializes arguments as raw Java objects; this method
     * wraps them into {@link ColumnValue} instances before delegating.</p>
     *
     * @param functionName the function name (case-insensitive)
     * @param arguments    positional argument values as raw Java objects
     * @return the result as a raw Java object (unwrapped from ColumnValue)
     * @throws NativeRuntimeException if the function is not found or evaluation fails
     */
    public Object evaluateScalar(String functionName, Object[] arguments) {
        String key = functionName.toLowerCase(java.util.Locale.ROOT);
        ScalarFunction fn = scalarFunctions.get(key);
        if (fn == null) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Scalar function not registered: " + functionName);
        }
        try {
            List<ColumnValue> args = wrapArguments(arguments);
            ColumnValue result = fn.invoke(args);
            return ColumnValue.toRaw(result);
        } catch (NativeRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Scalar function '" + functionName + "' evaluation failed: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Creates a new accumulator for a registered aggregate function.
     *
     * @param functionName the function name (case-insensitive)
     * @return the opaque accumulator object
     * @throws NativeRuntimeException if the function is not found
     */
    public Object createAccumulator(String functionName) {
        String key = functionName.toLowerCase(java.util.Locale.ROOT);
        AggregateFunction fn = aggregateFunctions.get(key);
        if (fn == null) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Aggregate function not registered: " + functionName);
        }
        return fn.createAccumulator();
    }

    /**
     * Adds an input row to an aggregate function accumulator.
     *
     * @param functionName the function name (case-insensitive)
     * @param accumulator  the accumulator created by {@link #createAccumulator}
     * @param arguments    positional argument values as raw Java objects
     * @throws NativeRuntimeException if the function is not found or evaluation fails
     */
    public void addInput(String functionName, Object accumulator, Object[] arguments) {
        String key = functionName.toLowerCase(java.util.Locale.ROOT);
        AggregateFunction fn = aggregateFunctions.get(key);
        if (fn == null) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Aggregate function not registered: " + functionName);
        }
        try {
            List<ColumnValue> args = wrapArguments(arguments);
            fn.addInput(accumulator, args);
        } catch (NativeRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Aggregate function '" + functionName + "' addInput failed: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Extracts the final result from an aggregate function accumulator.
     *
     * @param functionName the function name (case-insensitive)
     * @param accumulator  the accumulator after all rows have been processed
     * @return the aggregate result as a raw Java object
     * @throws NativeRuntimeException if the function is not found or evaluation fails
     */
    public Object getAggregateResult(String functionName, Object accumulator) {
        String key = functionName.toLowerCase(java.util.Locale.ROOT);
        AggregateFunction fn = aggregateFunctions.get(key);
        if (fn == null) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Aggregate function not registered: " + functionName);
        }
        try {
            ColumnValue result = fn.getResult(accumulator);
            return ColumnValue.toRaw(result);
        } catch (NativeRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new NativeRuntimeException(
                    org.pangrova.common.exception.PangrovaErrorCode.NATIVE_JNI_EXCEPTION,
                    "Aggregate function '" + functionName + "' getResult failed: " + e.getMessage(),
                    e);
        }
    }

    // ---- Query methods ----

    /**
     * Returns the number of registered scalar functions.
     */
    public int scalarCount() {
        return scalarFunctions.size();
    }

    /**
     * Returns the number of registered aggregate functions.
     */
    public int aggregateCount() {
        return aggregateFunctions.size();
    }

    /**
     * Returns an unmodifiable view of registered scalar function names.
     */
    public java.util.Set<String> scalarNames() {
        return Collections.unmodifiableSet(scalarFunctions.keySet());
    }

    /**
     * Returns an unmodifiable view of registered aggregate function names.
     */
    public java.util.Set<String> aggregateNames() {
        return Collections.unmodifiableSet(aggregateFunctions.keySet());
    }

    /**
     * Returns the scalar function registered under the given name, or null.
     */
    public ScalarFunction getScalar(String name) {
        return scalarFunctions.get(name.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Returns the aggregate function registered under the given name, or null.
     */
    public AggregateFunction getAggregate(String name) {
        return aggregateFunctions.get(name.toLowerCase(java.util.Locale.ROOT));
    }

    // ---- Internal helpers ----

    private static List<ColumnValue> wrapArguments(Object[] arguments) {
        if (arguments == null || arguments.length == 0) {
            return List.of();
        }
        var wrapped = new java.util.ArrayList<ColumnValue>(arguments.length);
        for (Object arg : arguments) {
            wrapped.add(ColumnValue.of(arg));
        }
        return wrapped;
    }
}
