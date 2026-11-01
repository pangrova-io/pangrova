package org.pangrova.worker.nativebridge.jni;

import java.util.List;
import java.util.Objects;

import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.NativeFunctionDescriptor.Builder;
import org.pangrova.protocol.v2.NativeFunctionKind;

/**
 * Describes a user-defined function to register with the native execution engine.
 *
 * <p>This is the wire-format-friendly descriptor that the {@link NativeFunctionBridge}
 * sends to the C++ side via JNI. It carries enough type information for the native engine to
 * create a proxy function that calls back into Java for evaluation.</p>
 *
 * <p>The {@code functionType} distinguishes scalar from aggregate functions,
 * which follow different native registration paths (native SimpleFunctionFactory
 * vs. AggregateFunctionFactory).</p>
 *
 * @param name          function name as it appears in SQL (case-insensitive matching)
 * @param returnType    return type name (e.g. "BIGINT", "VARCHAR")
 * @param argumentTypes ordered list of argument type names
 * @param functionType  SCALAR or AGGREGATE
 * @param defaultNullBehavior whether NULL input skips invocation and produces SQL NULL
 * @param deterministic whether the function is deterministic (enables expression folding)
 */
public record NativeFunctionDescriptor(
        String name,
        String returnType,
        List<String> argumentTypes,
        FunctionType functionType,
        boolean deterministic,
        boolean defaultNullBehavior) {

    /** Pure functions use the unified strict NULL-input default. */
    public NativeFunctionDescriptor(String name, String returnType, List<String> argumentTypes,
                                    FunctionType functionType, boolean deterministic) {
        this(name, returnType, argumentTypes, functionType, deterministic,
                new org.pangrova.spi.function.metadata.FunctionMetadata(name, returnType, argumentTypes)
                        .defaultNullBehavior());
    }

    public enum FunctionType {
        SCALAR,
        AGGREGATE
    }

    public NativeFunctionDescriptor {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(returnType, "returnType must not be null");
        Objects.requireNonNull(functionType, "functionType must not be null");
        argumentTypes = argumentTypes == null ? List.of() : List.copyOf(argumentTypes);
    }

    /**
     * Serializes this descriptor to a compact proto-like byte array for JNI transfer.
     *
     * <p>Wire format (proto3-compatible):
     * <ul>
     *   <li>Field 1 (string): name</li>
     *   <li>Field 2 (message): canonical LogicalType return type</li>
     *   <li>Field 3 (repeated message): ordered canonical LogicalType arguments</li>
     *   <li>Field 4 (enum): canonical NativeFunctionKind</li>
     *   <li>Field 5 (varint): deterministic (0=false, 1=true)</li>
     *   <li>Field 6 (optional varint): explicit default NULL behavior</li>
     * </ul>
     */
    public byte[] toBytes() {
        Builder builder = org.pangrova.protocol.v2.NativeFunctionDescriptor.newBuilder()
                .setName(name)
                .setReturnType(logicalType(returnType))
                .setFunctionKind(functionType == FunctionType.SCALAR
                        ? NativeFunctionKind.NATIVE_FUNCTION_KIND_SCALAR
                        : NativeFunctionKind.NATIVE_FUNCTION_KIND_AGGREGATE)
                .setDeterministic(deterministic)
                .setDefaultNullBehavior(defaultNullBehavior);
        argumentTypes.stream().map(NativeFunctionDescriptor::logicalType)
                .forEach(builder::addArgumentTypes);
        return builder.build().toByteArray();
    }

    private static LogicalType logicalType(String canonicalSql) {
        return LogicalType.newBuilder().setCanonicalSql(canonicalSql).build();
    }
}
