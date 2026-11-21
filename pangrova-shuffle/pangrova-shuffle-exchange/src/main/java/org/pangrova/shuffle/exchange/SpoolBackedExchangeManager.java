package org.pangrova.shuffle.exchange;

import org.pangrova.shuffle.exchange.enums.SpoolSinkState;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;

import org.pangrova.batch.api.enums.BatchFormat;
import org.pangrova.batch.api.page.NativePage;
import org.pangrova.shuffle.api.identity.ExchangeChannelContext;
import org.pangrova.shuffle.api.spool.SpoolReadSelection;
import org.pangrova.shuffle.api.spool.SpoolReader;
import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolStoreContext;
import org.pangrova.shuffle.api.spool.SpoolStoreFactory;
import org.pangrova.shuffle.api.spool.SpoolWriter;
import org.pangrova.shuffle.exchange.enums.ExchangeReadMode;

/**
 * {@link ExchangeChannelManager} backed by a {@link SpoolStore} (ADR-002 D4).
 *
 * <p>The sink writes Java pages to the supplied store; the source reads its committed segments.
 * This manager is not wired into Native execution. Local spool integration requires an explicit
 * producer-attempt selection and exchange publication protocol before it can claim Native or
 * retry support. The production remote egress is a separate provider path.
 *
 * <p>Thread-safe: SpoolStore implementations are required to be thread-safe.
 */
public final class SpoolBackedExchangeManager implements ExchangeChannelManager {

    private final SpoolStore spoolStore;
    private final Function<ExchangeChannelContext, SpoolReadSelection> selections;

    /**
     * Opens the explicitly selected Java-page store from providers visible to the supplied loader.
     * Validates the complete inventory before allocating storage. The returned manager owns the store.
     */
    public static SpoolBackedExchangeManager open(SpoolStoreContext context, ClassLoader loader,
            Function<ExchangeChannelContext, SpoolReadSelection> selections) {
        Objects.requireNonNull(loader, "loader");
        return open(context, ServiceLoader.load(SpoolStoreFactory.class, loader), selections);
    }

    static SpoolBackedExchangeManager open(SpoolStoreContext context, Iterable<SpoolStoreFactory> providers,
            Function<ExchangeChannelContext, SpoolReadSelection> selections) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(selections, "selections");
        var factories = new HashMap<String, SpoolStoreFactory>();
        for (var provider : providers) {
            String id = provider.name();
            if (provider.apiVersion() != SpoolStoreFactory.API_VERSION) {
                throw new IllegalArgumentException("Incompatible spool provider: " + id);
            }
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("Blank spool provider ID: " + provider.getClass().getName());
            }
            if (factories.putIfAbsent(id, provider) != null) {
                throw new IllegalArgumentException("Duplicate spool provider ID: " + id);
            }
        }
        var selected = factories.get(context.storeName());
        if (selected == null) {
            throw new IllegalArgumentException("Unknown spool provider: " + context.storeName());
        }
        return new SpoolBackedExchangeManager(selected.create(context), selections);
    }

    /** The exchange owner supplies an exact selection; there is no latest-attempt default. */
    SpoolBackedExchangeManager(SpoolStore spoolStore,
            Function<ExchangeChannelContext, SpoolReadSelection> selections) {
        this.spoolStore = Objects.requireNonNull(spoolStore, "spoolStore");
        this.selections = Objects.requireNonNull(selections, "selections");
    }

    @Override
    public ExchangeSink createSink(ExchangeChannelContext context) {
        Objects.requireNonNull(context, "context");
        // This is the Java-page write path. Native producer integration is not installed.
        return new SpoolBackedExchangeSink(spoolStore, context);
    }

    @Override
    public ExchangeSource createSource(ExchangeChannelContext context, ExchangeReadMode readMode) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(readMode, "readMode");
        if (readMode != ExchangeReadMode.BLOCKING) {
            throw new UnsupportedOperationException("Local spool storage does not implement streaming reads");
        }
        var selected = Objects.requireNonNull(selections.apply(context), "selection");
        if (!selected.queryId().equals(context.queryId()) || selected.stageId() != context.stageId()
                || selected.partitionId() != context.exchangePartitionId()) {
            throw new IllegalArgumentException("Spool selection belongs to another exchange partition");
        }
        return new SpoolBackedExchangeSource(spoolStore, selected);
    }

    @Override
    public void close() {
        spoolStore.close();
    }

    // --- Inner classes ---

    /**
     * ExchangeSink that writes pages to a SpoolStore.
     * Each page is framed within one attempt/partition segment, completed only at finish.
     */
    private static final class SpoolBackedExchangeSink implements ExchangeSink {
        private final SpoolStore store;
        private final ExchangeChannelContext context;
        private SpoolSinkState status = SpoolSinkState.OPEN;
        private RuntimeException failure;

        private SpoolWriter writer;

        SpoolBackedExchangeSink(SpoolStore store, ExchangeChannelContext context) {
            this.store = store;
            this.context = context;
        }

        @Override
        public synchronized void addNativePage(NativePage page) {
            requireOpen();
            if (page.format() != BatchFormat.ARROW_IPC) {
                throw new UnsupportedOperationException("Java spool exchange requires ARROW_IPC batches");
            }
            byte[] data = page.serializedBatch();
            try {
                writer().write(ByteBuffer.wrap(data));
            } catch (RuntimeException writeFailure) {
                failure = writeFailure;
                throw writeFailure;
            }
        }

        @Override
        public synchronized void finish() {
            if (status == SpoolSinkState.FINISHED) return;
            requireOpen();
            try {
                writer().commit();
                status = SpoolSinkState.FINISHED;
            } catch (RuntimeException commitFailure) {
                failure = commitFailure;
                throw commitFailure;
            }
        }

        @Override
        public synchronized void abort() {
            if (status != SpoolSinkState.FINISHED) status = SpoolSinkState.ABORTED;
            closeWriter();
        }

        @Override
        public synchronized void close() {
            if (status != SpoolSinkState.FINISHED) status = SpoolSinkState.ABORTED;
            closeWriter();
        }

        private void requireOpen() {
            if (failure != null) throw failure;
            if (status != SpoolSinkState.OPEN) throw new IllegalStateException("Spool sink is not open");
        }

        private SpoolWriter writer() {
            if (writer == null) {
                writer = store.open(
                        context.queryId(), context.stageId(),
                        context.exchangePartitionId(), context.taskPartitionId(),
                        context.attemptId());
            }
            return writer;
        }

        private void closeWriter() {
            if (writer != null) {
                writer.close();
                writer = null;
            }
        }
    }

    /**
     * ExchangeSource that reads pages from a SpoolStore.
     * Reads the exact selected attempt segments in producer order.
     */
    private static final class SpoolBackedExchangeSource implements ExchangeSource {
        private final SpoolStore store;
        private final SpoolReadSelection selection;
        private SpoolReader reader;
        private RuntimeException failure;
        private boolean finished;

        SpoolBackedExchangeSource(
                SpoolStore store, SpoolReadSelection selection) {
            this.store = store;
            this.selection = selection;
        }

        @Override
        public synchronized NativePage getNextNativePage() {
            if (failure != null) throw failure;
            if (finished) return null;
            try {
                if (reader == null) reader = store.read(selection);
                if (!reader.hasNext()) {
                    finished = true;
                    return null;
                }
                ByteBuffer buffer = reader.next();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return new NativePage(bytes, -1, -1, BatchFormat.ARROW_IPC);
            } catch (RuntimeException readFailure) {
                failure = readFailure;
                finished = true;
                throw readFailure;
            }
        }

        @Override
        public synchronized boolean isFinished() {
            return finished;
        }

        @Override
        public synchronized void close() {
            finished = true;
            if (reader != null) {
                reader.close();
            }
        }
    }
}
