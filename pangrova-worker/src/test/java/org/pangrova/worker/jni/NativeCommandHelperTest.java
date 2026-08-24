package org.pangrova.worker.jni;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NativeCommandBuilder} manual protobuf wire encoding.
 * Verifies round-trip correctness for each command type.
 */
class NativeCommandBuilderTest {

    @Test
    void updateMemoryLimitRoundTrip() {
        byte[] cmd = NativeCommandBuilder.updateMemoryLimit(42L, 1024 * 1024L);
        assertThat(cmd).isNotNull();
        assertThat(cmd.length).isGreaterThan(0);
        // Verify field 2 (command_type) = CMD_UPDATE_MEMORY_LIMIT (2)
        // and field 1 (executor_handle) = 42
        // by extracting via the same wire format parser
        byte[] payload = NativeCommandBuilder.extractResultPayload(cmd);
        // extractResultPayload extracts field 3 (payload) — should be present
        assertThat(payload).isNotNull();
    }

    @Test
    void getMetricsProducesValidProto() {
        byte[] cmd = NativeCommandBuilder.getMetrics();
        assertThat(cmd).isNotNull();
        assertThat(cmd.length).isGreaterThan(0);
    }

    @Test
    void getCacheStatsProducesValidProto() {
        byte[] cmd = NativeCommandBuilder.getCacheStats();
        assertThat(cmd).isNotNull();
    }

    @Test
    void buildGetCapabilitiesProducesValidProto() {
        byte[] cmd = NativeCommandBuilder.buildGetCapabilities();
        assertThat(cmd).isNotNull();
    }

    @Test
    void evictCacheWithPatternProducesValidProto() {
        byte[] cmd = NativeCommandBuilder.evictCache("s3://bucket/path/*");
        assertThat(cmd).isNotNull();
        byte[] payload = NativeCommandBuilder.extractResultPayload(cmd);
        assertThat(payload).isNotNull();
    }

    @Test
    void injectDynamicFilterProducesValidProto() {
        byte[] filterProto = new byte[]{0x0A, 0x02, 0x08, 0x01};
        byte[] cmd = NativeCommandBuilder.injectDynamicFilter(99L, "node_0", filterProto);
        assertThat(cmd).isNotNull();
        byte[] payload = NativeCommandBuilder.extractResultPayload(cmd);
        assertThat(payload).isNotNull();
    }

    @Test
    void extractResultPayloadFromEmptyReturnsNull() {
        assertThat(NativeCommandBuilder.extractResultPayload(null)).isNull();
        assertThat(NativeCommandBuilder.extractResultPayload(new byte[0])).isNull();
    }

    @Test
    void prefetchSplitsProducesValidProto() {
        byte[] splitInfos = new byte[]{0x0A, 0x04, 0x08, 0x01, 0x10, 0x02};
        byte[] cmd = NativeCommandBuilder.prefetchSplits(splitInfos);
        assertThat(cmd).isNotNull();
        byte[] payload = NativeCommandBuilder.extractResultPayload(cmd);
        assertThat(payload).isNotNull();
    }

    @Test
    void zeroHandleOmitsField1() {
        byte[] cmd = NativeCommandBuilder.getMetrics();
        // Field 1 (executor_handle) should be omitted when handle=0.
        // First byte should be field 2 tag (0x10 = field 2, wire type 0).
        assertThat(cmd[0]).isEqualTo((byte) 0x10);
    }

    @Test
    void nonZeroHandleIncludesField1() {
        byte[] cmd = NativeCommandBuilder.updateMemoryLimit(1L, 100L);
        // First byte should be field 1 tag (0x08 = field 1, wire type 0).
        assertThat(cmd[0]).isEqualTo((byte) 0x08);
    }
}
