package org.pangrova.worker.property.result;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.StartResultServerRequest;
import org.pangrova.worker.result.NativeResultTransport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test: JNI command envelope round-trip for result server commands.
 * Validates Property 5: StartResultServerCommand proto encoding is deterministic.
 */
class ResultServerCommandPropertyTest {

    @Property(tries = 200)
    void startResultServerPayloadRoundTrip(@ForAll @IntRange(min = 0, max = 65535) int port) {
        StartResultServerRequest payload = NativeResultTransport.buildStartResultServerPayload(port);
        assertThat(payload).isNotNull();
        assertThat(payload.getPreferredPort()).isEqualTo(port);
    }

    @Property(tries = 200)
    void commandEnvelopeContainsTypedPayload(
            @ForAll @IntRange(min = 0, max = 65535) int port) throws Exception {
        StartResultServerRequest payload = NativeResultTransport.buildStartResultServerPayload(port);
        byte[] command = NativeResultTransport.buildCommand(
                NativeResultTransport.CMD_START_RESULT_SERVER, payload);
        assertThat(command).isNotNull();
        RuntimeCommand decoded = RuntimeCommand.parseFrom(command);
        assertThat(decoded.getCommandType())
                .isEqualTo(NativeResultTransport.CMD_START_RESULT_SERVER);
        assertThat(decoded.getPayload().is(StartResultServerRequest.class)).isTrue();
        assertThat(decoded.getPayload().unpack(StartResultServerRequest.class).getPreferredPort())
                .isEqualTo(port);
    }

    @Property(tries = 200)
    void payloadEncodingIsDeterministic(@ForAll @IntRange(min = 0, max = 65535) int port) {
        byte[] first = NativeResultTransport.buildStartResultServerPayload(port).toByteArray();
        byte[] second = NativeResultTransport.buildStartResultServerPayload(port).toByteArray();
        assertThat(first).isEqualTo(second);
    }
}
