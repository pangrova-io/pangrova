package org.pangrova.worker.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectWriter;
import org.pangrova.common.json.provider.jackson.JsonMapperFactory;
import org.pangrova.protocol.rest.shuffle.PartitionStats;
import org.pangrova.protocol.rest.shuffle.ShuffleStageStatsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker-side encoder for {@link ShuffleStageStatsReport}.
 *
 * <p>Serializes shuffle stage statistics into JSON bytes for HTTP POST
 * to the Coordinator. No scheduling or decision logic — pure encoding path.</p>
 *
 * <p>Thread-safe: uses a shared {@link ObjectWriter} which is immutable.</p>
 */
public final class ShuffleStatsEncoder {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleStatsEncoder.class);
    private static final ObjectWriter WRITER = JsonMapperFactory.writer();

    private ShuffleStatsEncoder() {}

    /**
     * Encodes a {@link ShuffleStageStatsReport} to JSON bytes.
     *
     * @param report the report to encode (must not be null)
     * @return JSON byte array
     * @throws IOException if serialization fails
     */
    public static byte[] encode(ShuffleStageStatsReport report) throws IOException {
        checkNotNull(report, "report must not be null");
        return WRITER.writeValueAsBytes(report);
    }

    /**
     * Builds and encodes a {@link ShuffleStageStatsReport} from raw values.
     *
     * @param queryId        query identifier
     * @param stageId        stage identifier
     * @param partitionStats per-partition statistics
     * @param shuffleBytes   total shuffled bytes
     * @param shuffleRows    total shuffled rows
     * @return JSON byte array
     * @throws IOException if serialization fails
     */
    public static byte[] encode(String queryId, String stageId,
                                Map<Integer, PartitionStats> partitionStats,
                                long shuffleBytes, long shuffleRows) throws IOException {
        ShuffleStageStatsReport report = new ShuffleStageStatsReport(
                queryId, stageId, partitionStats,
                shuffleBytes, shuffleRows, System.currentTimeMillis());
        byte[] encoded = WRITER.writeValueAsBytes(report);
        LOG.debug("event=shuffle_stats_encoded queryId={} stageId={} "
                + "shuffleBytes={} shuffleRows={} encodedSize={}",
                queryId, stageId, shuffleBytes, shuffleRows, encoded.length);
        return encoded;
    }
}
