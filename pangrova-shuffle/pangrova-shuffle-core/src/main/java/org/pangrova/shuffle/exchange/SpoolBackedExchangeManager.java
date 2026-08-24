package org.pangrova.shuffle.exchange;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.pangrova.shuffle.spool.SpoolReader;
import org.pangrova.shuffle.spool.SpoolStore;
import org.pangrova.shuffle.spool.SpoolWriter;
import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;
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
    public ExchangeSink createSink(String queryId, int partitionId) {
        // For native execution, D2-egress handles the write path.
        // This sink exists for JVM-to-JVM fallback and test scenarios.
        return new SpoolBackedExchangeSink(spoolStore, queryId, partitionId);
    }

    @Override
    public ExchangeSource createSource(String queryId, int partitionId, ExchangeReadMode readMode) {
        // Read all committed segments for this partition across all map tasks.
        return new SpoolBackedExchangeSource(spoolStore, queryId, partitionId, defaultMapCount);
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
        private final String queryId;
        private final int partitionId;
        private volatile boolean sealed;
        private int mapId = 0;

        SpoolBackedExchangeSink(SpoolStore store, String queryId, int partitionId) {
            this.store = store;
            this.queryId = queryId;
            this.partitionId = partitionId;
        }

        @Override
        public void addNativePage(NativePage page) {
            if (sealed) {
                throw new IllegalStateException("Sink sealed");
            }
            byte[] data = page.serializedBatch();
            try (SpoolWriter writer = store.open(queryId, 0, partitionId, mapId++, 0L)) {
                writer.write(ByteBuffer.wrap(data));
                writer.commit();
            }
        }

        @Override
        public void finish() {
            sealed = true;
        }

        @Override
        public void abort() {
            sealed = true;
        }

        @Override
        public void close() {
            sealed = true;
        }
    }

    /**
     * ExchangeSource that reads pages from a SpoolStore.
     * Reads all committed segments for a partition in map-index order.
     */
    private static final class SpoolBackedExchangeSource implements ExchangeSource {
        private final SpoolStore store;
        private final String queryId;
        private final int partitionId;
        private final int endMapIndex;
        private volatile SpoolReader reader;
        private boolean finished;

        SpoolBackedExchangeSource(SpoolStore store, String queryId, int partitionId, int endMapIndex) {
            this.store = store;
            this.queryId = queryId;
            this.partitionId = partitionId;
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
                        r = store.read(queryId, 0, partitionId, 0, endMapIndex);
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
