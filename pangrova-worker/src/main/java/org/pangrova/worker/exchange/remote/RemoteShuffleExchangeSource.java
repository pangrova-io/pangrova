package org.pangrova.worker.exchange.remote;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.v1.shuffle.ReadBatchResponse;
import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.shuffle.exchange.ExchangeSource;
import org.pangrova.shuffle.exchange.ExchangeReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ExchangeSource} backed by a Remote Shuffle Service server.
 *
 * <p>Reads columnar IPC data from a remote shuffle server via gRPC. Supports two read modes:
 * <ul>
 *   <li>BLOCKING — uses {@code ReadPartition} RPC which blocks until the partition
 *       is committed on the server. Maps to hash/range distribution.</li>
 *   <li>STREAMING — uses {@code ReadPartitionStreaming} RPC which delivers data
 *       as it arrives, before commit. Maps to broadcast/singleton distribution.</li>
 * </ul>
 *
 * <p>The read mode is determined by the edge's {@link ExchangeReadMode}, derived
 * from the distribution type at the coordinator level.</p>
 *
 * <p>Thread-safe: uses {@link AtomicBoolean} for terminal state tracking.</p>
 */
@ThreadSafe
public final class RemoteShuffleExchangeSource implements ExchangeSource {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleExchangeSource.class);

    private final String shuffleId;
    private final int partitionId;
    private final RemoteShuffleClient client;
    private final ExchangeReadMode readMode;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Lazily initialized iterator from the gRPC server-streaming response. */
    private volatile Iterator<ReadBatchResponse> responseIterator;

    /** Creates a source reading one partition of a shuffle exchange. */
    public RemoteShuffleExchangeSource(String shuffleId, int partitionId,
                             RemoteShuffleClient client, ExchangeReadMode readMode) {
        this.shuffleId = checkNotNull(shuffleId, "shuffleId must not be null");
        this.partitionId = partitionId;
        this.client = checkNotNull(client, "client must not be null");
        this.readMode = readMode != null ? readMode : ExchangeReadMode.BLOCKING;
    }

    @Override
    public NativePage getNextNativePage() {
        if (closed.get() || finished.get()) {
            return null;
        }
        try {
            return readNextPage();
        } catch (RemoteShuffleClientException e) {
            LOG.error("event=remote_shuffle_source_read_failed partition={} error={}", partitionId, e.getMessage(), e);
            throw new RuntimeException("Shuffle read failed for partition " + partitionId, e);
        }
    }

    private NativePage readNextPage() {
        Iterator<ReadBatchResponse> iter = ensureIterator();
        if (!iter.hasNext()) {
            finished.set(true);
            return null;
        }
        ReadBatchResponse resp = iter.next();
        return convertResponse(resp);
    }

    private NativePage convertResponse(ReadBatchResponse resp) {
        byte[] data = resp.getArrowIpcData().toByteArray();
        if (resp.getIsLast()) {
            finished.set(true);
        }
        if (data.length == 0) {
            return finished.get() ? null : new NativePage(data, -1, -1, BatchFormat.ARROW_IPC);
        }
        LOG.debug("event=remote_shuffle_source_read shuffleId={} partitionId={} bytes={} isLast={}",
                shuffleId, partitionId, data.length, resp.getIsLast());
        return new NativePage(data, -1, -1, BatchFormat.ARROW_IPC);
    }

    @Override
    public boolean isFinished() {
        return finished.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        finished.set(true);
        LOG.debug("event=remote_shuffle_source_closed shuffleId={} partitionId={}", shuffleId, partitionId);
    }

    /** Returns the read mode for this source. */
    public ExchangeReadMode getReadMode() {
        return readMode;
    }

    private Iterator<ReadBatchResponse> ensureIterator() {
        if (responseIterator == null) {
            synchronized (this) {
                if (responseIterator == null) {
                    responseIterator = (readMode == ExchangeReadMode.STREAMING)
                            ? client.readPartitionStreaming(shuffleId, partitionId)
                            : client.readPartition(shuffleId, partitionId);
                }
            }
        }
        return responseIterator;
    }
}
