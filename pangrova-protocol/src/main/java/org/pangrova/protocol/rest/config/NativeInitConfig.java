package org.pangrova.protocol.rest.config;

import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration passed to NativeRuntime.nativeInit() for native runtime setup.
 * Field layout aligns with Protobuf NativeInitConfig (common.proto) and
 * native NativeConfig (pangrova_native_protocol.proto) for cross-boundary compatibility.
 * All fields are optional with sensible defaults.
 */
public record NativeInitConfig(
        @JsonProperty("max_memory_bytes") long maxMemoryBytes,
        @JsonProperty("io_thread_count") int ioThreadCount,
        @JsonProperty("cache_enabled") boolean cacheEnabled,
        @JsonProperty("ram_cache_bytes") long ramCacheBytes,
        @JsonProperty("ssd_cache_bytes") long ssdCacheBytes,
        @JsonProperty("ssd_cache_path") String ssdCachePath
) {

    public static final long DEFAULT_MAX_MEMORY_BYTES = 8L * 1024 * 1024 * 1024;
    public static final int DEFAULT_IO_THREAD_COUNT = 4;
    public static final long DEFAULT_RAM_CACHE_BYTES = 8L * 1024 * 1024 * 1024;
    public static final long DEFAULT_SSD_CACHE_BYTES = 100L * 1024 * 1024 * 1024;
    public static final String DEFAULT_SSD_CACHE_PATH =
            Path.of(System.getProperty("java.io.tmpdir"), "pangrova-ssd-cache").toString();

    public NativeInitConfig {
        if (maxMemoryBytes <= 0) {
            maxMemoryBytes = DEFAULT_MAX_MEMORY_BYTES;
        }
        if (ioThreadCount <= 0) {
            ioThreadCount = DEFAULT_IO_THREAD_COUNT;
        }
        if (ssdCachePath == null) {
            ssdCachePath = "";
        }
    }

    /**
     * Returns a configuration populated entirely with default values.
     */
    public static NativeInitConfig defaults() {
        return new NativeInitConfig(
                DEFAULT_MAX_MEMORY_BYTES,
                DEFAULT_IO_THREAD_COUNT,
                false,
                DEFAULT_RAM_CACHE_BYTES,
                DEFAULT_SSD_CACHE_BYTES,
                DEFAULT_SSD_CACHE_PATH
        );
    }
}
