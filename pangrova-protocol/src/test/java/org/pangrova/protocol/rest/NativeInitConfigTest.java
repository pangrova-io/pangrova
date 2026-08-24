package org.pangrova.protocol.rest;

import java.nio.file.Path;

import org.pangrova.protocol.rest.config.NativeInitConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies NativeInitConfig record defaults and protobuf serialization round-trip.
 * Field layout must match native proto NativeConfig for cross-boundary compatibility.
 */
class NativeInitConfigTest {

    @Test
    void defaultsApplyWhenFieldsAreZeroOrNull() {
        NativeInitConfig config = new NativeInitConfig(0, 0, false, 0, 0, null);

        assertThat(config.maxMemoryBytes()).isEqualTo(NativeInitConfig.DEFAULT_MAX_MEMORY_BYTES);
        assertThat(config.ioThreadCount()).isEqualTo(NativeInitConfig.DEFAULT_IO_THREAD_COUNT);
        assertThat(config.cacheEnabled()).isFalse();
        assertThat(config.ramCacheBytes()).isZero();
        assertThat(config.ssdCacheBytes()).isZero();
        assertThat(config.ssdCachePath()).isEmpty();
    }

    @Test
    void factoryMethodReturnsDefaults() {
        NativeInitConfig config = NativeInitConfig.defaults();

        assertThat(config.maxMemoryBytes()).isEqualTo(8L * 1024 * 1024 * 1024);
        assertThat(config.ioThreadCount()).isEqualTo(4);
        assertThat(config.cacheEnabled()).isFalse();
        assertThat(config.ramCacheBytes()).isEqualTo(8L * 1024 * 1024 * 1024);
        assertThat(config.ssdCacheBytes()).isEqualTo(100L * 1024 * 1024 * 1024);
        assertThat(config.ssdCachePath()).isEqualTo(
            Path.of(System.getProperty("java.io.tmpdir"), "pangrova-ssd-cache").toString());
    }

    @Test
    void customValuesPreserved() {
        NativeInitConfig config = new NativeInitConfig(
                1024L * 1024 * 1024, 8, true,
                4L * 1024 * 1024 * 1024, 50L * 1024 * 1024 * 1024,
                "/data/ssd-cache");

        assertThat(config.maxMemoryBytes()).isEqualTo(1024L * 1024 * 1024);
        assertThat(config.ioThreadCount()).isEqualTo(8);
        assertThat(config.cacheEnabled()).isTrue();
        assertThat(config.ramCacheBytes()).isEqualTo(4L * 1024 * 1024 * 1024);
        assertThat(config.ssdCacheBytes()).isEqualTo(50L * 1024 * 1024 * 1024);
        assertThat(config.ssdCachePath()).isEqualTo("/data/ssd-cache");
    }

    @Test
    void protobufRoundTrip() {
        org.pangrova.protocol.v1.NativeInitConfig proto = org.pangrova.protocol.v1.NativeInitConfig.newBuilder()
                .setMaxMemoryBytes(2L * 1024 * 1024 * 1024)
                .setIoThreadCount(16)
                .setCacheEnabled(true)
                .setRamCacheBytes(4L * 1024 * 1024 * 1024)
                .setSsdCacheBytes(50L * 1024 * 1024 * 1024)
                .setSsdCachePath("/data/ssd")
                .build();

        byte[] bytes = proto.toByteArray();
        assertThat(bytes).isNotEmpty();

        org.pangrova.protocol.v1.NativeInitConfig parsed = parseProto(bytes);

        assertThat(parsed.getMaxMemoryBytes()).isEqualTo(2L * 1024 * 1024 * 1024);
        assertThat(parsed.getIoThreadCount()).isEqualTo(16);
        assertThat(parsed.getCacheEnabled()).isTrue();
        assertThat(parsed.getRamCacheBytes()).isEqualTo(4L * 1024 * 1024 * 1024);
        assertThat(parsed.getSsdCacheBytes()).isEqualTo(50L * 1024 * 1024 * 1024);
        assertThat(parsed.getSsdCachePath()).isEqualTo("/data/ssd");
    }

    @Test
    void emptyBytesProduceDefaultProto() {
        org.pangrova.protocol.v1.NativeInitConfig parsed = parseProto(new byte[0]);

        assertThat(parsed.getMaxMemoryBytes()).isZero();
        assertThat(parsed.getIoThreadCount()).isZero();
        assertThat(parsed.getCacheEnabled()).isFalse();
        assertThat(parsed.getRamCacheBytes()).isZero();
        assertThat(parsed.getSsdCacheBytes()).isZero();
        assertThat(parsed.getSsdCachePath()).isEmpty();
    }

    private static org.pangrova.protocol.v1.NativeInitConfig parseProto(byte[] bytes) {
        try {
            return org.pangrova.protocol.v1.NativeInitConfig.parseFrom(bytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse NativeInitConfig proto", e);
        }
    }
}
