package org.pangrova.worker.jni;

/**
 * Command type constants for the native unified command channel.
 * Values must match NativeCommandType enum in pangrova_native_protocol.proto.
 *
 * <p>Centralizes command type constants to eliminate magic numbers
 * scattered across transport implementations. All native command
 * dispatchers should reference these constants instead of inline ints.</p>
 */
public final class NativeCommandType {

    private NativeCommandType() {}

    public static final int INJECT_DYNAMIC_FILTER = 1;
    public static final int UPDATE_MEMORY_LIMIT = 2;
    public static final int PREFETCH_SPLITS = 3;
    public static final int EVICT_CACHE = 4;
    public static final int GET_METRICS = 5;
    public static final int GET_CACHE_STATS = 6;
    public static final int GET_CAPABILITIES = 7;
    public static final int MEMORY_PRESSURE = 8;
    // 9 is reserved in native proto NativeCommandType enum — do not reuse.
    public static final int START_SHUFFLE_SERVER = 10;
    public static final int CONNECT_EXCHANGE_SOURCE = 11;
    public static final int START_RESULT_SERVER = 12;
    public static final int STOP_RESULT_SERVER = 13;
    public static final int START_NATIVE_EXCHANGE = 14;
    public static final int STOP_SHUFFLE_SERVER = 15;
    public static final int NO_MORE_EXCHANGE_SOURCES = 16;
}
