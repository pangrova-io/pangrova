/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.shuffle.celeborn;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.pangrova.shuffle.exchange.ExchangeChannelManager;
import org.pangrova.shuffle.exchange.ExchangeReadMode;
import org.pangrova.shuffle.exchange.ExchangeSink;
import org.pangrova.shuffle.exchange.ExchangeSource;
import org.pangrova.spi.connector.data.page.BatchFormat;
import org.pangrova.spi.connector.data.page.NativePage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Celeborn implementation of {@link ExchangeChannelManager} (ADR-002 D4).
 *
 * <p>Data plane: maps Pangrova exchange operations to Celeborn's push/pull model:
 * <ul>
 *   <li>{@code createSink} → wraps {@code CelebornClientWrapper.pushData()}</li>
 *   <li>{@code createSource} → wraps {@code CelebornClientWrapper.readPartition()}</li>
 * </ul>
 *
 * <p>Unlike the SpoolBacked exchange manager which uses local files, Celeborn's exchange
 * pushes data to remote workers during write and pulls from them during read. The sink
 * pushes each NativePage immediately; the source reads on demand.
 */
public final class CelebornExchangeChannelManager implements ExchangeChannelManager {

    private static final Logger LOG = LoggerFactory.getLogger(CelebornExchangeChannelManager.class);

    private final CelebornClientWrapper clientWrapper;
    private final CelebornShuffleIdResolver shuffleIdResolver;

    public CelebornExchangeChannelManager(CelebornClientWrapper clientWrapper,
                                           CelebornShuffleIdResolver shuffleIdResolver) {
        this.clientWrapper = Objects.requireNonNull(clientWrapper, "clientWrapper");
        this.shuffleIdResolver = Objects.requireNonNull(shuffleIdResolver, "shuffleIdResolver");
    }

    @Override
    public ExchangeSink createSink(String queryId, int partitionId) {
        LOG.debug("Creating Celeborn exchange sink: queryId={}, partitionId={}", queryId, partitionId);
        return new CelebornExchangeSink(clientWrapper, shuffleIdResolver, queryId, partitionId);
    }

    @Override
    public ExchangeSource createSource(String queryId, int partitionId, ExchangeReadMode readMode) {
        LOG.debug("Creating Celeborn exchange source: queryId={}, partitionId={}, mode={}",
                queryId, partitionId, readMode);
        return new CelebornExchangeSource(clientWrapper, shuffleIdResolver, queryId, partitionId);
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing CelebornExchangeChannelManager");
        // Client wrapper lifecycle is managed externally by the provider.
    }

    // ---- Inner classes: Sink and Source ----

    /**
     * Exchange sink that pushes NativePage data to Celeborn workers.
     */
    private static final class CelebornExchangeSink implements ExchangeSink {

        private final CelebornClientWrapper clientWrapper;
        private final CelebornShuffleIdResolver resolver;
        private final String queryId;
        private final int partitionId;
        // mapId is fixed at 0 because the ExchangeChannelManager SPI does not expose mapper
        // identity. In the primary Celeborn data path (native → ByteEgress), the real mapId
        // is provided by the native layer. This fallback path is for Java-only operators.
        private static final int MAP_ID = 0;
        private boolean finished;
        private boolean aborted;

        CelebornExchangeSink(CelebornClientWrapper clientWrapper,
                             CelebornShuffleIdResolver resolver,
                             String queryId, int partitionId) {
            this.clientWrapper = clientWrapper;
            this.resolver = resolver;
            this.queryId = queryId;
            this.partitionId = partitionId;
        }

        @Override
        public void addNativePage(NativePage page) {
            if (finished || aborted) {
                throw new IllegalStateException("Sink already " + (finished ? "finished" : "aborted"));
            }
            Objects.requireNonNull(page, "page must not be null");

            int shuffleId = resolver.resolveShuffleId(queryId);
            byte[] data = page.serializedBatch();
            clientWrapper.pushData(shuffleId, MAP_ID, 0, partitionId, ByteBuffer.wrap(data));
        }

        @Override
        public void finish() {
            if (finished || aborted) {
                throw new IllegalStateException("Sink already " + (finished ? "finished" : "aborted"));
            }
            finished = true;
            // Celeborn pushes data immediately; finish is a no-op for the data path.
        }

        @Override
        public void abort() {
            aborted = true;
            // Data already pushed cannot be retracted; Celeborn will handle cleanup
            // when the shuffle is unregistered.
        }

        @Override
        public void close() {
            if (!finished && !aborted) {
                abort();
            }
        }
    }

    /**
     * Exchange source that reads partition data from Celeborn workers in streaming chunks.
     *
     * <p>Instead of reading the entire partition into memory at once (which risks OOM for
     * large partitions), this source opens a streaming handle on first access and reads
     * data in fixed-size chunks ({@link #CHUNK_SIZE_BYTES}). Each call to
     * {@code getNextNativePage()} returns one chunk until the stream is exhausted.
     */
    private static final class CelebornExchangeSource implements ExchangeSource {

        /** Default chunk size: 8 MB — aligned with Celeborn's internal block size. */
        private static final int CHUNK_SIZE_BYTES = 8 * 1024 * 1024;

        private final CelebornClientWrapper clientWrapper;
        private final CelebornShuffleIdResolver resolver;
        private final String queryId;
        private final int partitionId;
        private InputStream partitionStream;
        private boolean finished;

        CelebornExchangeSource(CelebornClientWrapper clientWrapper,
                               CelebornShuffleIdResolver resolver,
                               String queryId, int partitionId) {
            this.clientWrapper = clientWrapper;
            this.resolver = resolver;
            this.queryId = queryId;
            this.partitionId = partitionId;
        }

        @Override
        public NativePage getNextNativePage() {
            if (finished) {
                return null;
            }

            try {
                if (partitionStream == null) {
                    int shuffleId = resolver.resolveShuffleId(queryId);
                    partitionStream = clientWrapper.openPartitionStream(shuffleId, partitionId);
                }

                byte[] chunk = partitionStream.readNBytes(CHUNK_SIZE_BYTES);
                if (chunk.length == 0) {
                    finished = true;
                    closeStream();
                    return null;
                }

                // rowCount=0, columnCount=0: exact metadata unknown without parsing Arrow IPC header.
                // Downstream consumers must handle 0 as "not-yet-determined" and parse if needed.
                return new NativePage(chunk, 0, 0, BatchFormat.ARROW_IPC);
            } catch (IOException e) {
                finished = true;
                closeStream();
                throw new CelebornOperationException(
                        "Failed to read partition stream: queryId=" + queryId
                        + " partition=" + partitionId, e);
            }
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void close() {
            finished = true;
            closeStream();
        }

        private void closeStream() {
            if (partitionStream != null) {
                try {
                    partitionStream.close();
                } catch (IOException e) {
                    // Best-effort close; stream may already be exhausted.
                }
                partitionStream = null;
            }
        }
    }
}
