package org.pangrova.common.format;

/**
 * Determines whether native column format support is available in this deployment.
 *
 * <p>Support for native-only column formats (e.g., Lance C FFI) is a compile-time
 * dependency of the native binary (libpangrova_native). When the binary is built
 * without that support, the Java layer must reject DDL operations that reference
 * such formats at creation time rather than failing at query time.</p>
 *
 * <p>Availability is controlled by the system property {@code pangrova.native-format.available}.
 * Defaults to {@code true} (most production deployments include native format support).
 * Set to {@code "false"} for deployments where the native binary was built without the
 * corresponding C FFI linkage.</p>
 *
 * <p>This class is in {@code pangrova-common} so both the coordinator
 * (DDL validation) and connectors (split resolution) can check availability without
 * cross-module dependency violations.</p>
 */
public final class NativeFormatCapabilities {

    private static final String PROPERTY_KEY = "pangrova.native-format.available";

    private static volatile boolean available = resolveInitial();

    private NativeFormatCapabilities() {}

    /**
     * Returns {@code true} if native column format support is available in this deployment.
     *
     * <p>Thread-safe: reads a volatile field. The value can be updated at runtime
     * via {@link #setAvailable(boolean)} after native capability discovery.</p>
     */
    public static boolean isAvailable() {
        return available;
    }

    /**
     * Updates native format availability at runtime. Called by the worker after native
     * library initialization confirms whether the format's C FFI is linked.
     *
     * @param value {@code true} if native format C FFI is linked in the native binary
     */
    public static void setAvailable(boolean value) {
        available = value;
    }

    private static boolean resolveInitial() {
        String prop = System.getProperty(PROPERTY_KEY);
        if (prop == null) {
            return true;
        }
        return Boolean.parseBoolean(prop);
    }
}
