package org.pangrova.worker.property.result;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.worker.result.NativeResultTransport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test: JNI command envelope round-trip for result server commands.
 * Validates Property 5: StartResultServerCommand proto encoding is deterministic.
 */
class ResultServerCommandPropertyTest {

    @Property(tries = 200)
    void startResultServerPayloadRoundTrip(@ForAll @IntRange(min = 0, max = 65535) int port) {
        byte[] payload = NativeResultTransport.buildStartResultServerPayload(port);
        assertThat(payload).isNotNull();
        assertThat(payload.length).isGreaterThan(0);
        // Tag byte for field 1, wire type 0 = 0x08
        assertThat(payload[0]).isEqualTo((byte) 0x08);
    }

    @Property(tries = 200)
    void commandEnvelopeContainsCommandType(@ForAll @IntRange(min = 0, max = 65535) int port) {
        byte[] payload = NativeResultTransport.buildStartResultServerPayload(port);
        byte[] command = NativeResultTransport.buildCommand(
                0, NativeResultTransport.CMD_START_RESULT_SERVER, payload);
        assertThat(command).isNotNull();
        // Envelope must contain command type field (field 2, tag 0x10)
        boolean hasCommandTypeTag = false;
        for (byte b : command) {
            if (b == 0x10) { hasCommandTypeTag = true; break; }
        }
        assertThat(hasCommandTypeTag).isTrue();
    }

    @Property(tries = 200)
    void payloadEncodingIsDeterministic(@ForAll @IntRange(min = 0, max = 65535) int port) {
        byte[] first = NativeResultTransport.buildStartResultServerPayload(port);
        byte[] second = NativeResultTransport.buildStartResultServerPayload(port);
        assertThat(first).isEqualTo(second);
    }
}
