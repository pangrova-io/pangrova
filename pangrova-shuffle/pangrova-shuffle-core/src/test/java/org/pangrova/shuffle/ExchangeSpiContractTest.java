package org.pangrova.shuffle;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.shuffle.exchange.ExchangeChannelContext;
import org.pangrova.shuffle.exchange.ExchangeChannelManager;
import org.pangrova.shuffle.exchange.ExchangeAttemptFence;
import org.pangrova.shuffle.exchange.ExchangeReadMode;
import org.pangrova.shuffle.exchange.ExchangeSink;
import org.pangrova.shuffle.exchange.ExchangeSource;
import org.pangrova.shuffle.exchange.ExchangeTaskAttempt;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates the Exchange SPI lifecycle contract (columnar ArrowPage path):
 * create → write/read → finish/abort → close.
 */
class ExchangeSpiContractTest {

    @Test
    void sinkFinishMakesDataAvailableToSource() throws IOException {
        var manager = new InMemoryExchangeManager();
        var sink = manager.createSink(context("q1", 0));
        var source = manager.createSource(context("q1", 0));

        sink.addNativePage(testArrowPage("batch1"));
        sink.finish();

        NativePage result = source.getNextNativePage();
        assertThat(result).isNotNull();
        assertThat(result.serializedBatch()).isEqualTo("batch1".getBytes(StandardCharsets.UTF_8));
        assertThat(source.isFinished()).isTrue();

        source.close();
        sink.close();
        manager.close();
    }

    @Test
    void sourceReturnsNullBeforeSinkFinishes() throws IOException {
        var manager = new InMemoryExchangeManager();
        var sink = manager.createSink(context("q1", 0));
        var source = manager.createSource(context("q1", 0));

        assertThat(source.getNextNativePage()).isNull();
        assertThat(source.isFinished()).isFalse();

        sink.addNativePage(testArrowPage("data"));
        sink.finish();

        assertThat(source.getNextNativePage()).isNotNull();
        assertThat(source.isFinished()).isTrue();

        source.close();
        sink.close();
        manager.close();
    }

    @Test
    void addPageAfterFinishThrowsIllegalState() throws IOException {
        var manager = new InMemoryExchangeManager();
        var sink = manager.createSink(context("q1", 0));

        sink.finish();

        assertThatThrownBy(() -> sink.addNativePage(testArrowPage("x")))
                .isInstanceOf(IllegalStateException.class);

        sink.close();
        manager.close();
    }

    @Test
    void addPageAfterAbortThrowsIllegalState() throws IOException {
        var manager = new InMemoryExchangeManager();
        var sink = manager.createSink(context("q1", 0));

        sink.abort();

        assertThatThrownBy(() -> sink.addNativePage(testArrowPage("x")))
                .isInstanceOf(IllegalStateException.class);

        sink.close();
        manager.close();
    }

    @Test
    void abortDiscardsBufferedData() throws IOException {
        var manager = new InMemoryExchangeManager();
        var sink = manager.createSink(context("q1", 0));
        var source = manager.createSource(context("q1", 0));

        sink.addNativePage(testArrowPage("data"));
        sink.abort();

        assertThat(source.getNextNativePage()).isNull();
        assertThat(source.isFinished()).isTrue();

        source.close();
        sink.close();
        manager.close();
    }

    @Test
    void multiplePartitionsAreIndependent() throws IOException {
        var manager = new InMemoryExchangeManager();
        var sink0 = manager.createSink(context("q1", 0));
        var sink1 = manager.createSink(context("q1", 1));
        var source0 = manager.createSource(context("q1", 0));
        var source1 = manager.createSource(context("q1", 1));

        sink0.addNativePage(testArrowPage("p0"));
        sink0.finish();
        sink1.addNativePage(testArrowPage("p1"));
        sink1.finish();

        NativePage page0 = source0.getNextNativePage();
        NativePage page1 = source1.getNextNativePage();

        assertThat(page0).isNotNull();
        assertThat(page0.serializedBatch()).isEqualTo("p0".getBytes(StandardCharsets.UTF_8));
        assertThat(page1).isNotNull();
        assertThat(page1.serializedBatch()).isEqualTo("p1".getBytes(StandardCharsets.UTF_8));

        source0.close();
        source1.close();
        sink0.close();
        sink1.close();
        manager.close();
    }

    // ---- helpers ----

    private static NativePage testArrowPage(String content) {
        return new NativePage(content.getBytes(StandardCharsets.UTF_8), -1, -1, BatchFormat.ARROW_IPC);
    }

    private static ExchangeChannelContext context(String queryId, int partitionId) {
        return new ExchangeChannelContext(
                new ExchangeTaskAttempt(
                        queryId, 0, 0, "task-0", 1L,
                        new ExchangeAttemptFence(1L, 0, 1L),
                        new ShuffleExecutionOwner("owner", "incarnation", 1L)),
                partitionId,
                Long.MAX_VALUE);
    }

    // ---- Minimal in-memory implementation ----

    private static final class InMemoryExchangeManager implements ExchangeChannelManager {
        private final java.util.concurrent.ConcurrentHashMap<String, InMemoryChannel> channels =
                new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public ExchangeSink createSink(ExchangeChannelContext context) {
            var channel = channels.computeIfAbsent(
                    context.queryId() + ":" + context.exchangePartitionId(),
                    k -> new InMemoryChannel());
            return new InMemorySink(channel);
        }

        @Override
        public ExchangeSource createSource(
                ExchangeChannelContext context, ExchangeReadMode readMode) {
            var channel = channels.computeIfAbsent(
                    context.queryId() + ":" + context.exchangePartitionId(),
                    k -> new InMemoryChannel());
            return new InMemorySource(channel);
        }

        @Override
        public void close() { channels.clear(); }
    }

    private static final class InMemoryChannel {
        final ConcurrentLinkedQueue<NativePage> pages = new ConcurrentLinkedQueue<>();
        final AtomicBoolean finished = new AtomicBoolean(false);
        final AtomicBoolean aborted = new AtomicBoolean(false);
    }

    private static final class InMemorySink implements ExchangeSink {
        private final InMemoryChannel channel;
        private boolean sealed;

        InMemorySink(InMemoryChannel channel) { this.channel = channel; }

        @Override
        public void addNativePage(NativePage page) {
            if (sealed) {
                throw new IllegalStateException("Sink already sealed");
            }
            channel.pages.add(page);
        }

        @Override
        public void finish() {
            if (sealed) throw new IllegalStateException("Already sealed");
            sealed = true;
            channel.finished.set(true);
        }

        @Override
        public void abort() {
            sealed = true;
            channel.aborted.set(true);
            channel.finished.set(true);
            channel.pages.clear();
        }

        @Override
        public void close() {}
    }

    private static final class InMemorySource implements ExchangeSource {
        private final InMemoryChannel channel;

        InMemorySource(InMemoryChannel channel) { this.channel = channel; }

        @Override
        public NativePage getNextNativePage() { return channel.pages.poll(); }

        @Override
        public boolean isFinished() {
            return channel.finished.get() && channel.pages.isEmpty();
        }

        @Override
        public void close() {}
    }
}
