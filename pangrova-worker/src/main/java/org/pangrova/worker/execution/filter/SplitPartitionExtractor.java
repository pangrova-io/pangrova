package org.pangrova.worker.execution.filter;

import java.util.Base64;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts partition values from Base64-encoded canonical v2 DataSplit bytes.
 *
 * <p>Used by {@link DynamicFilterApplicator} to evaluate dynamic filters
 * against split partition metadata before passing splits to native execution.</p>
 *
 * <p>Thread safety: stateless utility — safe to call from any thread.</p>
 */
final class SplitPartitionExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(SplitPartitionExtractor.class);

    private SplitPartitionExtractor() {}

    /**
     * Extracts partition_values from a Base64-encoded SplitInfo protobuf.
     *
     * @param encodedSplit Base64-encoded SplitInfo proto bytes
     * @return partition values map (column name → value string), empty if extraction fails
     */
    static Map<String, String> extractPartitionValues(String encodedSplit) {
        if (encodedSplit == null || encodedSplit.isEmpty()) {
            return Map.of();
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(encodedSplit);
            return DataSplitProtoCodec.stringPartitionValues(DataSplit.parseFrom(bytes));
        } catch (IllegalArgumentException e) {
            LOG.warn("event=split_decode_failed error={}", e.getMessage());
            return Map.of();
        } catch (InvalidProtocolBufferException e) {
            LOG.warn("event=split_parse_failed error={}", e.getMessage());
            return Map.of();
        }
    }
}
