package org.pangrova.worker.result;

import org.junit.jupiter.api.Test;
import com.google.protobuf.Any;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.StartResultServerRequest;
import org.pangrova.protocol.v2.StartResultServerResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for NativeResultTransport proto encoding and response parsing.
 * Tests the Java-side wire format without requiring the native library.
 */
class NativeResultTransportTest {

    @Test
    void buildStartResultServerPayload_port0_encodesCorrectly() {
        StartResultServerRequest payload = NativeResultTransport.buildStartResultServerPayload(0);
        assertThat(payload.getPreferredPort()).isZero();
        assertThat(payload.getBindHost()).isEqualTo("0.0.0.0");
    }

    @Test
    void buildStartResultServerPayload_port8080_encodesCorrectly() {
        StartResultServerRequest payload = NativeResultTransport.buildStartResultServerPayload(8080);
        assertThat(payload.getPreferredPort()).isEqualTo(8080);
        assertThat(payload.getBindHost()).isEqualTo("0.0.0.0");
    }

    @Test
    void explicitListenerAddressIsPreservedInCanonicalCommand() {
        StartResultServerRequest payload = NativeResultTransport.buildStartResultServerPayload(
                0, "127.0.0.1");
        assertThat(payload.getBindHost()).isEqualTo("127.0.0.1");
    }

    @Test
    void buildCommand_withPayload_encodesEnvelope() throws Exception {
        StartResultServerRequest payload = StartResultServerRequest.newBuilder().build();
        byte[] command = NativeResultTransport.buildCommand(
                NativeResultTransport.CMD_START_RESULT_SERVER, payload);
        RuntimeCommand decoded = RuntimeCommand.parseFrom(command);
        assertThat(decoded.getProtocol().getMajor()).isEqualTo(2);
        assertThat(decoded.getPayload().unpack(StartResultServerRequest.class))
                .isEqualTo(payload);
    }

    @Test
    void buildCommand_withoutPayload_encodesMinimalEnvelope() throws Exception {
        byte[] command = NativeResultTransport.buildCommand(
                NativeResultTransport.CMD_STOP_RESULT_SERVER, null);
        assertThat(RuntimeCommand.parseFrom(command).hasPayload()).isFalse();
    }

    @Test
    void extractBoundPort_validResponse_returnsPort() {
        StartResultServerResponse innerPayload = StartResultServerResponse.newBuilder()
                .setBoundPort(9999).build();
        // Wrap in a successful generated NativeCommandResponse.
        byte[] envelope = generatedResponseEnvelope(innerPayload);
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
        try (var executor = new org.pangrova.worker.execution.NativeTaskExecutor();
             var transport = new NativeResultTransport(executor, "localhost")) {
            assertThat(transport.boundPort()).isEqualTo(-1);
        }
    }

    /**
     * Generated protobuf boundary input, not a fabricated task execution.
     */
    private static byte[] generatedResponseEnvelope(StartResultServerResponse resultPayload) {
        return RuntimeCommandResponse.newBuilder()
                .setSuccess(true)
                .setPayload(Any.pack(resultPayload))
                .build()
                .toByteArray();
    }
}
