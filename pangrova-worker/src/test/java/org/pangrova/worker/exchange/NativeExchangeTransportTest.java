package org.pangrova.worker.exchange;

import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.shuffle.exchange.ExchangeEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link NativeExchangeTransport}.
 *
 * <p>Verifies that startExchange delegates to JNI executeCommand with the
 * correct command type, addSources issues per-source JNI commands, and
 * input validation works correctly.</p>
 */
class NativeExchangeTransportTest {

    private CommandCapturingExecutor executor;
    private NativeExchangeTransport transport;

    @BeforeEach
    void setUp() {
        executor = new CommandCapturingExecutor();
        transport = new NativeExchangeTransport(executor);
    }

    @Test
    void startExchange_delegatesToJni() {
        long handle = executor.createTask(new byte[0]);
        List<ExchangeEndpoint> sources = List.of(
                new ExchangeEndpoint("task-1", "host-1", 8080, 0),
                new ExchangeEndpoint("task-2", "host-2", 8081, 1));

        transport.startExchange(handle, sources);

        assertThat(executor.capturedCommands).hasSize(1);
        assertThat(executor.capturedCommands.get(0).executorHandle()).isEqualTo(handle);
    }

    @Test
    void startExchange_rejectsZeroHandle() {
        List<ExchangeEndpoint> sources = List.of(
                new ExchangeEndpoint("task-1", "host-1", 8080, 0));

        assertThatThrownBy(() -> transport.startExchange(0, sources))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("executorHandle must not be zero");
    }

    @Test
    void startExchange_rejectsNullSources() {
        long handle = executor.createTask(new byte[0]);

        assertThatThrownBy(() -> transport.startExchange(handle, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void startExchange_acceptsEmptySources() {
        long handle = executor.createTask(new byte[0]);

        transport.startExchange(handle, List.of());

        assertThat(executor.capturedCommands).hasSize(1);
    }

    @Test
    void addSources_issuesPerSourceCommand() {
        long handle = executor.createTask(new byte[0]);
        List<ExchangeEndpoint> newSources = List.of(
                new ExchangeEndpoint("task-3", "host-3", 8082, 0),
                new ExchangeEndpoint("task-4", "host-4", 8083, 1));

        transport.addSources(handle, newSources);

        // One command per source
        assertThat(executor.capturedCommands).hasSize(2);
    }

    @Test
    void addSources_rejectsNullSources() {
        long handle = executor.createTask(new byte[0]);

        assertThatThrownBy(() -> transport.addSources(handle, null))
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
    void constructorRejectsNullExecutor() {
        assertThatThrownBy(() -> new NativeExchangeTransport(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("taskExecutor must not be null");
    }

    /**
     * TaskExecutor that captures executeCommand calls for assertion.
     */
    static final class CommandCapturingExecutor implements TaskExecutor {

        final List<CapturedCommand> capturedCommands = new ArrayList<>();
        private long nextHandle = 1;

        CommandCapturingExecutor() {}

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
            return new byte[0];
        }
    }
}
