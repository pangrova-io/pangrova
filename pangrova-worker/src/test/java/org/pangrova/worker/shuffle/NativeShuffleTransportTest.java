package org.pangrova.worker.shuffle;

import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.shuffle.ShuffleEndpoint;
import org.pangrova.shuffle.ShuffleServerHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link NativeShuffleTransport}.
 *
 * <p>Verifies that startServer returns a valid handle with the correct host/port,
 * connectSource delegates to JNI executeCommand, and input validation works.</p>
 *
 * <p>Uses a {@link CommandCapturingExecutor} that captures JNI commands and
 * returns a valid StartFlightServerResponse with a bound port.</p>
 */
class NativeShuffleTransportTest {

    private CommandCapturingExecutor executor;
    private NativeShuffleTransport transport;

    @BeforeEach
    void setUp() {
        executor = new CommandCapturingExecutor(9999);
        transport = new NativeShuffleTransport(executor, "test-host");
    }

    @Test
    void startServer_returnsValidHandle() {
        long handle = executor.createTask(new byte[0]);

        ShuffleServerHandle serverHandle = transport.startServer(handle, 4);

        assertThat(serverHandle).isNotNull();
        assertThat(serverHandle.host()).isEqualTo("test-host");
        assertThat(serverHandle.port()).isEqualTo(9999);
    }

    @Test
    void startServer_sendsStartFlightServerCommand() {
        long handle = executor.createTask(new byte[0]);

        transport.startServer(handle, 4);

        assertThat(executor.capturedCommands).hasSize(1);
        CommandCapturingExecutor.CapturedCommand cmd = executor.capturedCommands.get(0);
        assertThat(cmd.executorHandle()).isEqualTo(handle);
        assertThat(cmd.commandBytes()).isNotEmpty();
    }

    @Test
    void startServer_rejectsZeroHandle() {
        assertThatThrownBy(() -> transport.startServer(0, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("executorHandle must not be zero");
    }

    @Test
    void startServer_rejectsNonPositivePartitions() {
        long handle = executor.createTask(new byte[0]);

        assertThatThrownBy(() -> transport.startServer(handle, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numPartitions must be positive");

        assertThatThrownBy(() -> transport.startServer(handle, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("numPartitions must be positive");
    }

    @Test
    void connectSource_delegatesToJni() {
        long handle = executor.createTask(new byte[0]);
        ShuffleEndpoint endpoint = new ShuffleEndpoint(
                "upstream-host", 8080, "q1", 0, 0);

        transport.connectSource(handle, "source-1", endpoint);

        assertThat(executor.capturedCommands).hasSize(1);
        CommandCapturingExecutor.CapturedCommand cmd = executor.capturedCommands.get(0);
        assertThat(cmd.executorHandle()).isEqualTo(handle);
    }

    @Test
    void connectSource_rejectsNullSourceId() {
        long handle = executor.createTask(new byte[0]);
        ShuffleEndpoint endpoint = new ShuffleEndpoint(
                "upstream-host", 8080, "q1", 0, 0);

        assertThatThrownBy(() -> transport.connectSource(handle, null, endpoint))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void connectSource_rejectsNullEndpoint() {
        long handle = executor.createTask(new byte[0]);

        assertThatThrownBy(() -> transport.connectSource(handle, "source-1", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void noMoreSources_doesNotThrow() {
        long handle = executor.createTask(new byte[0]);
        transport.noMoreSources(handle);
        // Issues NO_MORE_EXCHANGE_SOURCES command to native side
        assertThat(executor.capturedCommands).hasSize(1);
        assertThat(executor.capturedCommands.get(0).executorHandle()).isEqualTo(handle);
    }

    @Test
    void close_doesNotThrow() {
        transport.close();
        // Idempotent
        transport.close();
    }

    @Test
    void defaultHost_usesLocalhost() {
        NativeShuffleTransport nullHostTransport =
                new NativeShuffleTransport(executor, null);
        long handle = executor.createTask(new byte[0]);

        ShuffleServerHandle serverHandle = nullHostTransport.startServer(handle, 1);

        assertThat(serverHandle.host()).isEqualTo("localhost");
    }

    /**
     * TaskExecutor that captures executeCommand calls and returns a valid
     * StartFlightServerResponse with a configurable bound port.
     */
    static final class CommandCapturingExecutor implements TaskExecutor {

        final List<CapturedCommand> capturedCommands = new ArrayList<>();
        private final int boundPort;
        private long nextHandle = 1;

        CommandCapturingExecutor(int boundPort) {
            this.boundPort = boundPort;
        }

        record CapturedCommand(long executorHandle, byte[] commandBytes) {}

        @Override
        public long createTask(byte[] taskRequestBytes) {
            return nextHandle++;
        }

        @Override
        public void startTask(long executorHandle) {}

        @Override
        public void releaseTask(long executorHandle) {}

        @Override
        public void cancelTask(long executorHandle) {}

        @Override
        public byte[] executeCommand(long executorHandle, byte[] commandProto) {
            capturedCommands.add(new CapturedCommand(executorHandle, commandProto));
            return buildFlightServerResponse(boundPort);
        }

        /**
         * Builds a NativeCommandResponse proto with:
         *   field 1 (status): varint 0 (OK)
         *   field 3 (result_payload): StartFlightServerResponse { field 1 (bound_port): varint }
         */
        private static byte[] buildFlightServerResponse(int port) {
            try {
                // Build inner payload: StartFlightServerResponse { bound_port }
                ByteArrayOutputStream inner = new ByteArrayOutputStream(8);
                writeTag(inner, 1, 0); // field 1, wire type 0
                writeVarint(inner, port);
                byte[] payload = inner.toByteArray();

                // Build outer response: NativeCommandResponse
                ByteArrayOutputStream outer = new ByteArrayOutputStream(32);
                writeTag(outer, 1, 0); // field 1 (status), wire type 0
                writeVarint(outer, 0); // OK
                writeTag(outer, 3, 2); // field 3 (result_payload), wire type 2
                writeVarint(outer, payload.length);
                outer.write(payload);
                return outer.toByteArray();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private static void writeTag(ByteArrayOutputStream out, int fieldNumber,
                                      int wireType) throws IOException {
            writeVarint(out, (fieldNumber << 3) | wireType);
        }

        private static void writeVarint(ByteArrayOutputStream out, long value)
                throws IOException {
            while ((value & ~0x7FL) != 0) {
                out.write((int) ((value & 0x7F) | 0x80));
                value >>>= 7;
            }
            out.write((int) value);
        }
    }
}
