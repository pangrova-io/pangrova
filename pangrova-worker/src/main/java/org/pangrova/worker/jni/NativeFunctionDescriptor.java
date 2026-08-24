package org.pangrova.worker.jni;

import java.util.List;
import java.util.Objects;

import org.pangrova.worker.jni.proto.NativeFunctionDescriptorProto;

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
 * @param deterministic whether the function is deterministic (enables expression folding)
 */
public record NativeFunctionDescriptor(
        String name,
        String returnType,
        List<String> argumentTypes,
        FunctionType functionType,
        boolean deterministic) {

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
     *   <li>Field 2 (string): returnType</li>
     *   <li>Field 3 (repeated string): argumentTypes</li>
     *   <li>Field 4 (varint): functionType ordinal (0=SCALAR, 1=AGGREGATE)</li>
     *   <li>Field 5 (varint): deterministic (0=false, 1=true)</li>
     * </ul>
     */
    public byte[] toBytes() {
        NativeFunctionDescriptorProto.Builder builder = NativeFunctionDescriptorProto.newBuilder()
                .setName(name)
                .setReturnType(returnType)
                .addAllArgumentTypes(argumentTypes)
                .setFunctionType(functionType.ordinal())
                .setDeterministic(deterministic ? 1 : 0);
        return builder.build().toByteArray();
    }
}
