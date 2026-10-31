package org.pangrova.shuffle.exchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.connector.spi.data.page.BatchFormat;
import org.pangrova.connector.spi.data.page.NativePage;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;

/**
 * Property-based tests for Exchange SPI contract completeness (columnar path).
 *
 * <p><b>Validates: Requirements 3.1, 3.2</b></p>
 */
class ExchangeSpiPropertyTest {

    /**
     * Property 4: Exchange SPI columnar contract — data written via sink
     * must be fully readable via source, no loss, no duplication.
     *
     * <p><b>Validates: Requirements 3.1, 3.2</b></p>
     */
    @Property(tries = 10)
    void dataWrittenToSinkIsFullyReadableFromSource(
            @ForAll @IntRange(min = 0, max = 10) int pageCount,
            @ForAll @IntRange(min = 0, max = 5) int partitionId) throws IOException {

        var manager = new InMemoryExchangeManager();
        var sink = manager.createSink(context("prop-query", partitionId));
        var source = manager.createSource(context("prop-query", partitionId));

        List<byte[]> written = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            byte[] ipc = ("batch_" + i).getBytes(StandardCharsets.UTF_8);
            sink.addNativePage(new NativePage(ipc, -1, -1, BatchFormat.ARROW_IPC));
            written.add(ipc);
        }
        sink.finish();

        List<NativePage> read = new ArrayList<>();
        NativePage page;
        while ((page = source.getNextNativePage()) != null) {
            read.add(page);
        }

        assertThat(source.isFinished()).isTrue();
        assertThat(read).hasSize(written.size());
        for (int i = 0; i < written.size(); i++) {
            assertThat(read.get(i).serializedBatch()).isEqualTo(written.get(i));
        }

        source.close();
        sink.close();
        manager.close();
    }

    /**
     * Property: Abort discards all buffered data for any number of pages.
     *
     * <p><b>Validates: Requirements 3.1, 3.2</b></p>
     */
    @Property(tries = 10)
    void abortDiscardsAllBufferedData(
            @ForAll @IntRange(min = 1, max = 10) int pageCount) throws IOException {

        var manager = new InMemoryExchangeManager();
        var sink = manager.createSink(context("abort-query", 0));
        var source = manager.createSource(context("abort-query", 0));

        for (int i = 0; i < pageCount; i++) {
            sink.addNativePage(new NativePage(
                    ("d" + i).getBytes(StandardCharsets.UTF_8), -1, -1, BatchFormat.ARROW_IPC));
        }
        sink.abort();

        assertThat(source.getNextNativePage()).isNull();
        assertThat(source.isFinished()).isTrue();

        source.close();
        sink.close();
        manager.close();
    }

    // ---- Minimal in-memory implementation ----

    private static final class InMemoryExchangeManager implements ExchangeChannelManager {
        private final ConcurrentHashMap<String, InMemoryChannel> channels = new ConcurrentHashMap<>();

        @Override
        public ExchangeSink createSink(ExchangeChannelContext context) {
            return new InMemorySink(
                    channels.computeIfAbsent(
                            context.queryId() + ":" + context.exchangePartitionId(),
                            k -> new InMemoryChannel()));
        }

        @Override
        public ExchangeSource createSource(
                ExchangeChannelContext context, ExchangeReadMode readMode) {
            return new InMemorySource(
                    channels.computeIfAbsent(
                            context.queryId() + ":" + context.exchangePartitionId(),
                            k -> new InMemoryChannel()));
        }

        @Override
        public void close() { channels.clear(); }
    }

    private static ExchangeChannelContext context(String queryId, int partitionId) {
        return new ExchangeChannelContext(
                new ExchangeTaskAttempt(
                        queryId, 0, 0, "task-0", 1L,
                        new ExchangeAttemptFence(1L, 0, 1L),
                        new ShuffleExecutionOwner("owner", "incarnation", 1L),
                        "00000000-0000-0000-0000-000000000001",
                        "00000000-0000-0000-0000-000000000002"),
                partitionId,
                Long.MAX_VALUE);
    }

    private static final class InMemoryChannel {
        final ConcurrentLinkedQueue<NativePage> pages = new ConcurrentLinkedQueue<>();
        final AtomicBoolean finished = new AtomicBoolean(false);
    }

    private static final class InMemorySink implements ExchangeSink {
        private final InMemoryChannel channel;
        private boolean sealed;

        InMemorySink(InMemoryChannel channel) { this.channel = channel; }

        @Override
        public void addNativePage(NativePage page) {
            if (sealed) throw new IllegalStateException("Sink sealed");
            channel.pages.add(page);
        }

        @Override
        public void finish() { sealed = true; channel.finished.set(true); }

        @Override
        public void abort() { sealed = true; channel.pages.clear(); channel.finished.set(true); }

        @Override
        public void close() {}
    }

    private static final class InMemorySource implements ExchangeSource {
        private final InMemoryChannel channel;

        InMemorySource(InMemoryChannel channel) { this.channel = channel; }

        @Override
        public NativePage getNextNativePage() { return channel.pages.poll(); }

        @Override
        public boolean isFinished() { return channel.finished.get() && channel.pages.isEmpty(); }

        @Override
        public void close() {}
    }
}
