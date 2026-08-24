package org.pangrova.worker.result;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for NativeResultTransport proto encoding and response parsing.
 * Tests the Java-side wire format without requiring the native library.
 */
class NativeResultTransportTest {

    @Test
    void buildStartResultServerPayload_port0_encodesCorrectly() {
        byte[] payload = NativeResultTransport.buildStartResultServerPayload(0);
        assertThat(payload).isNotEmpty();
        // field 1 tag (0x08) + varint 0 (0x00)
        assertThat(payload[0]).isEqualTo((byte) 0x08);
        assertThat(payload[1]).isEqualTo((byte) 0x00);
    }

    @Test
    void buildStartResultServerPayload_port8080_encodesCorrectly() {
        byte[] payload = NativeResultTransport.buildStartResultServerPayload(8080);
        assertThat(payload).isNotEmpty();
        assertThat(payload[0]).isEqualTo((byte) 0x08);
    }

    @Test
    void buildCommand_withPayload_encodesEnvelope() {
        byte[] payload = new byte[]{0x08, 0x00};
        byte[] command = NativeResultTransport.buildCommand(
                0, NativeResultTransport.CMD_START_RESULT_SERVER, payload);
        assertThat(command).isNotEmpty();
        // Should contain command type field (field 2) with value 12
        assertThat(command.length).isGreaterThan(4);
    }

    @Test
    void buildCommand_withoutPayload_encodesMinimalEnvelope() {
        byte[] command = NativeResultTransport.buildCommand(
                0, NativeResultTransport.CMD_STOP_RESULT_SERVER, null);
        assertThat(command).isNotEmpty();
    }

    @Test
    void extractBoundPort_validResponse_returnsPort() {
        // Build a mock response: NativeCommand envelope with result_payload containing
        // StartResultServerResponse { bound_port = 9999 }
        // result_payload field: tag 0x08 (field 1, varint) + varint 9999
        byte[] innerPayload = NativeResultTransport.buildStartResultServerPayload(9999);
        // Wrap in NativeCommand response envelope: field 4 (result_payload), wire type 2
        byte[] envelope = buildMockResponseEnvelope(innerPayload);
        int port = NativeResultTransport.extractBoundPort(envelope);
        assertThat(port).isEqualTo(9999);
    }

    @Test
    void extractBoundPort_emptyResponse_throws() {
        assertThatThrownBy(() -> NativeResultTransport.extractBoundPort(new byte[0]))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void boundPort_beforeStart_returnsNegative() {
        var transport = new NativeResultTransport(
                new org.pangrova.worker.execution.SimulatedTaskExecutor(0), "localhost");
        assertThat(transport.boundPort()).isEqualTo(-1);
    }

    /**
     * Builds a mock NativeCommandResponse envelope containing the given result_payload.
     * Format: field 3 (result_payload), wire type 2 (length-delimited).
     */
    private static byte[] buildMockResponseEnvelope(byte[] resultPayload) {
        // field 3, wire type 2 = (3 << 3) | 2 = 0x1A
        byte[] envelope = new byte[2 + resultPayload.length];
        envelope[0] = 0x1A; // tag
        envelope[1] = (byte) resultPayload.length; // length (assumes < 128)
        System.arraycopy(resultPayload, 0, envelope, 2, resultPayload.length);
        return envelope;
    }
}
