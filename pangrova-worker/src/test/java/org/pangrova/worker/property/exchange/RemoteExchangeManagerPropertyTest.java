package org.pangrova.worker.property.exchange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.shuffle.ShuffleExecutionOwner;
import org.pangrova.shuffle.exchange.ExchangeAttemptFence;
import org.pangrova.shuffle.exchange.ExchangeChannelContext;
import org.pangrova.shuffle.exchange.ExchangeChannelManager;
import org.pangrova.shuffle.exchange.ExchangeSource;
import org.pangrova.shuffle.exchange.ExchangeSink;
import org.pangrova.shuffle.exchange.ExchangeTaskAttempt;

/**
 * Property-based tests for Exchange abstraction layer (columnar ArrowPage path).
 *
 * <p><b>Validates: Requirements 3.3, 3.4, 3.5</b></p>
 */
class RemoteExchangeManagerPropertyTest {

    /**
     * Property 1: Data written to a sink is fully readable from a source.
     *
     * <p><b>Validates: Requirements 3.3, 3.4</b></p>
     */
    @Property(tries = 10)
    void sinkSourceRoundTripPreservesAllPages(
            @ForAll @IntRange(min = 0, max = 10) int pageCount,
            @ForAll @IntRange(min = 0, max = 5) int partitionId) throws IOException {

        ExchangeChannelManager manager = new InMemoryTestExchangeManager();
        String queryId = "prop-q-" + partitionId;

        ExchangeSink sink = manager.createSink(context(queryId, partitionId));
        ExchangeSource source = manager.createSource(context(queryId, partitionId));

        List<byte[]> written = writePages(sink, pageCount);
        sink.finish();

        List<NativePage> read = drainSource(source);

        assertThat(read).hasSize(written.size());
        for (int i = 0; i < written.size(); i++) {
            assertThat(read.get(i).serializedBatch()).isEqualTo(written.get(i));
        }

        source.close();
        sink.close();
        manager.close();
    }

    /**
     * Property 2: Abort discards all buffered data.
     *
     * <p><b>Validates: Requirements 3.3</b></p>
     */
    @Property(tries = 10)
    void abortDiscardsAllBufferedPages(
            @ForAll @IntRange(min = 1, max = 10) int pageCount) throws IOException {

        ExchangeChannelManager manager = new InMemoryTestExchangeManager();
        ExchangeSink sink = manager.createSink(context("abort-q", 0));
        ExchangeSource source = manager.createSource(context("abort-q", 0));

        writePages(sink, pageCount);
        sink.abort();

        assertThat(source.getNextNativePage()).isNull();
        assertThat(source.isFinished()).isTrue();

        source.close();
        sink.close();
        manager.close();
    }

    /**
     * Property 3: addNativePage after finish throws IllegalStateException.
     *
     * <p><b>Validates: Requirements 3.3</b></p>
     */
    @Property(tries = 10)
    void addPageAfterFinishThrows(
            @ForAll @IntRange(min = 0, max = 5) int preFinishPages) throws IOException {

        ExchangeChannelManager manager = new InMemoryTestExchangeManager();
        ExchangeSink sink = manager.createSink(context("sealed-q", 0));

        writePages(sink, preFinishPages);
        sink.finish();

        NativePage extra = buildArrowPage(99);
        assertThatThrownBy(() -> sink.addNativePage(extra))
                .isInstanceOf(IllegalStateException.class);

        sink.close();
        manager.close();
    }

    // ---- Helpers ----

    private static List<byte[]> writePages(ExchangeSink sink, int count) {
        List<byte[]> batchList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] batch = ("batch_" + i).getBytes(StandardCharsets.UTF_8);
            sink.addNativePage(new NativePage(batch, -1, -1, BatchFormat.ARROW_IPC));
            batchList.add(batch);
        }
        return batchList;
    }

    private static NativePage buildArrowPage(int index) {
        return new NativePage(
                ("batch_" + index).getBytes(StandardCharsets.UTF_8), -1, -1, BatchFormat.ARROW_IPC);
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

    private static List<NativePage> drainSource(ExchangeSource source) {
        List<NativePage> result = new ArrayList<>();
        NativePage page;
        while ((page = source.getNextNativePage()) != null) {
            result.add(page);
        }
        return result;
    }

    // ---- In-memory ExchangeManager for property testing ----

    static final class InMemoryTestExchangeManager implements ExchangeChannelManager {
        private final ConcurrentHashMap<String, InMemoryChannel> channels = new ConcurrentHashMap<>();

        @Override
        public ExchangeSink createSink(ExchangeChannelContext context) {
            return new InMemoryTestSink(
                    channels.computeIfAbsent(
                            context.queryId() + ":" + context.exchangePartitionId(),
                            k -> new InMemoryChannel()));
        }

        @Override
        public ExchangeSource createSource(
                ExchangeChannelContext context,
                org.pangrova.shuffle.exchange.ExchangeReadMode readMode) {
            return new InMemoryTestSource(
                    channels.computeIfAbsent(
                            context.queryId() + ":" + context.exchangePartitionId(),
                            k -> new InMemoryChannel()));
        }

        @Override
        public void close() { channels.clear(); }
    }

    private static final class InMemoryChannel {
        final ConcurrentLinkedQueue<NativePage> pages = new ConcurrentLinkedQueue<>();
        final AtomicBoolean finished = new AtomicBoolean(false);
    }

    private static final class InMemoryTestSink implements ExchangeSink {
        private final InMemoryChannel channel;
        private final AtomicBoolean sealed = new AtomicBoolean(false);

        InMemoryTestSink(InMemoryChannel channel) { this.channel = channel; }

        @Override
        public void addNativePage(NativePage page) {
            if (sealed.get()) throw new IllegalStateException("Sink sealed");
            channel.pages.add(page);
        }

        @Override
        public void finish() { sealed.set(true); channel.finished.set(true); }

        @Override
        public void abort() { sealed.set(true); channel.pages.clear(); channel.finished.set(true); }

        @Override
        public void close() { sealed.set(true); }
    }

    private static final class InMemoryTestSource implements ExchangeSource {
        private final InMemoryChannel channel;

        InMemoryTestSource(InMemoryChannel channel) { this.channel = channel; }

        @Override
        public NativePage getNextNativePage() { return channel.pages.poll(); }

        @Override
        public boolean isFinished() { return channel.finished.get() && channel.pages.isEmpty(); }

        @Override
        public void close() {}
    }
}
