package org.pangrova.shuffle.core.exchange;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.pangrova.shuffle.api.spool.SpoolReader;
import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolWriter;
import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.shuffle.api.exchange.ExchangeChannelContext;
import org.pangrova.shuffle.api.exchange.ExchangeChannelManager;
import org.pangrova.shuffle.api.exchange.ExchangeReadMode;
import org.pangrova.shuffle.api.exchange.ExchangeSink;
import org.pangrova.shuffle.api.exchange.ExchangeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ExchangeChannelManager} backed by a {@link SpoolStore} (ADR-002 D4).
 *
 * <p>Used by both the {@code local-spill} and {@code object-store} shuffle providers.
 * The write path (sink) spools data to the SpoolStore, while the read path (source)
 * reads committed segments back from the SpoolStore.
 *
 * <p>For native execution with D2-egress enabled, the primary write path is through
 * {@code DefaultShuffleByteEgress} (JNI callback). The sink here serves as a fallback
 * for JVM-to-JVM writes and for coordinator-level metrics collection.
 *
 * <p>Thread-safe: SpoolStore implementations are required to be thread-safe.
 */
public final class SpoolBackedExchangeManager implements ExchangeChannelManager {

    private static final Logger LOG = LoggerFactory.getLogger(SpoolBackedExchangeManager.class);

    private final SpoolStore spoolStore;
    /** Default number of map tasks; used as endMapIndex for full-partition reads. */
    private final int defaultMapCount;

    public SpoolBackedExchangeManager(SpoolStore spoolStore) {
        this(spoolStore, Integer.MAX_VALUE);
    }

    public SpoolBackedExchangeManager(SpoolStore spoolStore, int defaultMapCount) {
        this.spoolStore = Objects.requireNonNull(spoolStore, "spoolStore");
        this.defaultMapCount = defaultMapCount;
    }

    @Override
    public ExchangeSink createSink(ExchangeChannelContext context) {
        Objects.requireNonNull(context, "context");
        // For native execution, D2-egress handles the write path.
        // This sink exists for JVM-to-JVM fallback and test scenarios.
        return new SpoolBackedExchangeSink(spoolStore, context);
    }

    @Override
    public ExchangeSource createSource(ExchangeChannelContext context, ExchangeReadMode readMode) {
        Objects.requireNonNull(context, "context");
        // Read all committed segments for this partition across all map tasks.
        return new SpoolBackedExchangeSource(spoolStore, context, defaultMapCount);
    }

    @Override
    public void close() {
        try {
            spoolStore.close();
        } catch (Exception e) {
            LOG.warn("event=spool_backed_exchange_manager_close_failed error={}", e.getMessage());
        }
    }

    // --- Inner classes ---

    /**
     * ExchangeSink that writes pages to a SpoolStore.
     * Each page is written as a separate segment (open-write-commit).
     */
    private static final class SpoolBackedExchangeSink implements ExchangeSink {
        private final SpoolStore store;
        private final ExchangeChannelContext context;
        private volatile boolean sealed;
        private SpoolWriter writer;

        SpoolBackedExchangeSink(SpoolStore store, ExchangeChannelContext context) {
            this.store = store;
            this.context = context;
        }

        @Override
        public synchronized void addNativePage(NativePage page) {
            if (sealed) {
                throw new IllegalStateException("Sink sealed");
            }
            byte[] data = page.serializedBatch();
            writer().write(ByteBuffer.wrap(data));
        }

        @Override
        public synchronized void finish() {
            if (sealed) {
                return;
            }
            writer().commit();
            sealed = true;
        }

        @Override
        public synchronized void abort() {
            closeWriter();
            sealed = true;
        }

        @Override
        public synchronized void close() {
            closeWriter();
            sealed = true;
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
     * Reads all committed segments for a partition in map-index order.
     */
    private static final class SpoolBackedExchangeSource implements ExchangeSource {
        private final SpoolStore store;
        private final ExchangeChannelContext context;
        private final int endMapIndex;
        private volatile SpoolReader reader;
        private boolean finished;

        SpoolBackedExchangeSource(
                SpoolStore store, ExchangeChannelContext context, int endMapIndex) {
            this.store = store;
            this.context = context;
            this.endMapIndex = endMapIndex;
        }

        @Override
        public NativePage getNextNativePage() {
            if (finished) {
                return null;
            }
            SpoolReader r = reader;
            if (r == null) {
                synchronized (this) {
                    r = reader;
                    if (r == null) {
                        // Lazy init: read all map segments for this partition.
                        r = store.read(
                                context.queryId(), context.stageId(),
                                context.exchangePartitionId(), 0, endMapIndex);
                        reader = r;
                    }
                }
            }
            try {
                if (!r.hasNext()) {
                    finished = true;
                    return null;
                }
                ByteBuffer buf = r.next();
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                return new NativePage(bytes, -1, -1, BatchFormat.ARROW_IPC);
            } catch (Exception e) {
                finished = true;
                throw new RuntimeException("SpoolBackedExchangeSource read failed", e);
            }
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void close() {
            finished = true;
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    LOG.warn("event=spool_backed_exchange_source_close_failed error={}",
                            e.getMessage());
                }
            }
        }
    }
}
