package org.pangrova.worker.exchange.remote;

import static org.pangrova.common.util.Preconditions.checkNotNull;
import static org.pangrova.common.util.Preconditions.checkState;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.v1.shuffle.CompressionCodec;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.shuffle.exchange.ExchangeSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ExchangeSink} backed by a Remote Shuffle Service server.
 *
 * <p>Writes columnar IPC data to a remote shuffle server via gRPC, with optional
 * compression and monotonic sequence numbering. On {@link #finish()}, commits
 * the partition on the server, making data available for downstream readers.</p>
 *
 * <p>Thread-safe: uses {@link AtomicBoolean} for sealed flag and
 * {@link AtomicLong} for sequence generation.</p>
 */
@ThreadSafe
public final class RemoteShuffleExchangeSink implements ExchangeSink {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleExchangeSink.class);

    private final String shuffleId;
    private final int partitionId;
    private final RemoteShuffleClient client;
    private final CompressionCodec codec;
    private final AtomicBoolean sealed = new AtomicBoolean(false);
    private final AtomicLong sequence = new AtomicLong(0);

    /** Creates a sink for one partition of a shuffle exchange. */
    public RemoteShuffleExchangeSink(String shuffleId, int partitionId,
                           RemoteShuffleClient client, CompressionCodec codec) {
        this.shuffleId = checkNotNull(shuffleId, "shuffleId must not be null");
        this.partitionId = partitionId;
        this.client = checkNotNull(client, "client must not be null");
        this.codec = codec != null ? codec : CompressionCodec.NONE;
    }

    @Override
    public void addNativePage(NativePage page) {
        checkNotNull(page, "page must not be null");
        checkState(!sealed.get(), "Sink already sealed for shuffleId=" + shuffleId);
        byte[] data = page.serializedBatch();
        client.writeBatch(shuffleId, partitionId,
                sequence.getAndIncrement(), data, data.length, codec);
    }

    @Override
    public void finish() {
        if (!sealed.compareAndSet(false, true)) {
            return;
        }
        client.commitPartition(shuffleId, partitionId);
        LOG.debug("event=remote_shuffle_sink_finished shuffleId={} partitionId={}", shuffleId, partitionId);
    }

    @Override
    public void abort() {
        sealed.set(true);
        LOG.debug("event=remote_shuffle_sink_aborted shuffleId={} partitionId={}", shuffleId, partitionId);
    }

    @Override
    public void close() {
        sealed.set(true);
    }

    /** Returns whether this sink has been sealed (finished or aborted). */
    boolean isSealed() {
        return sealed.get();
    }
}
