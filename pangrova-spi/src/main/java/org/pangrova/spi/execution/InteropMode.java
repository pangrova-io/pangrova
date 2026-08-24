package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Experimental;

/**
 * Defines the interoperability mode for the Java-to-native execution boundary.
 *
 * <p>Pangrova's architecture crosses a JNI boundary between the Java control plane
 * and the native C++ execution engine. The current implementation uses hand-written
 * per-batch JNI pull with two manually aligned protobuf schemas (Java proto in
 * {@code pangrova-protocol} and native proto in {@code pangrova-worker-native}).
 * This enum models the migration path toward the standard columnar data interface
 * (C Stream Interface), which eliminates the dual-schema alignment burden and
 * provides zero-copy columnar data transfer.</p>
 *
 * <p>The active mode is controlled by the {@code worker.native.interop-mode}
 * configuration property and reported via {@link ExecutionCapabilities}.</p>
 *
 * @see NativeColumnarStream
 * @see <a href="https://arrow.apache.org/docs/format/CStreamInterface.html">
 *      Columnar C Stream Interface specification</a>
 */
@Experimental(reason = "G-INTEROP-07: migration from legacy JNI pull to columnar C Stream Interface")
public enum InteropMode {

    /**
     * Legacy interop: hand-written JNI pull ABI with two manually aligned
     * protobuf schemas (Java-side {@code task.proto} and native-side
     * {@code pangrova_native_protocol.proto}). Field numbers must be kept
     * in sync across proto definitions per the contract in
     * {@code docs/architecture/20-java-native-protocol.md}.
     *
     * <p>This is the current production mode. Batch results cross the
     * JNI boundary as serialized byte arrays with a status-byte prefix
     * ({@link BatchResult}).</p>
     */
    LEGACY_JNI_PULL,

    /**
     * Target interop: columnar C Stream Interface (native stream struct ABI) for
     * zero-copy columnar transfer across the JNI boundary.
     *
     * <p>Uses a single shared Substrait plan schema. Data crosses the boundary
     * as native memory pointers (native array struct, native schema struct)
     * rather than serialized byte arrays.</p>
     *
     * <p>Benefits over {@link #LEGACY_JNI_PULL}:</p>
     * <ul>
     *   <li>Single schema source of truth (Substrait plan) — no dual-proto alignment</li>
     *   <li>Zero-copy transfer via columnar data interface pointers</li>
     *   <li>Standard ABI compatible with any columnar native engine</li>
     *   <li>Eliminates hand-written varint parsing in {@code NativeTaskExecutor}</li>
     * </ul>
     *
     * <p>Not yet implemented. Selecting this mode will throw
     * {@link UnsupportedOperationException} until the native side implements
     * the stream producer.</p>
     */
    NATIVE_C_STREAM;

    /**
     * Returns the {@link InteropMode} corresponding to the given configuration
     * string value. The match is case-insensitive and accepts both enum names
     * and kebab-case config values.
     *
     * @param value configuration string (e.g. "legacy-jni-pull", "native-c-stream",
     *              "LEGACY_JNI_PULL", "NATIVE_C_STREAM")
     * @return the matching interop mode
     * @throws IllegalArgumentException if the value does not match any known mode
     */
    public static InteropMode fromConfigValue(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("InteropMode config value must not be null or blank");
        }
        String normalized = value.strip().toUpperCase().replace('-', '_');
        try {
            return InteropMode.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown interop mode: '" + value + "'. "
                            + "Valid values: legacy-jni-pull, native-c-stream", e);
        }
    }

    /**
     * Returns the kebab-case configuration value for this mode,
     * suitable for YAML configuration files.
     *
     * @return kebab-case string (e.g. "legacy-jni-pull")
     */
    public String toConfigValue() {
        return name().toLowerCase().replace('_', '-');
    }
}
