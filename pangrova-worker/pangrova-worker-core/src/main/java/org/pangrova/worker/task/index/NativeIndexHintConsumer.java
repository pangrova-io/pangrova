package org.pangrova.worker.task.index;

import org.pangrova.protocol.index.IndexEvalHintProtoCodec;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.indexing.api.eval.IndexEvalHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Ensures index evaluation hints embedded in v2 {@link DataSplit} protos are
 * preserved and accessible for native engine page-level data skipping.
 *
 * <p>The coordinator serializes {@code IndexEvalHintInfo} messages into each
 * split via {@link IndexEvalHintProtoCodec}. This class validates that hints
 * survive the worker-side processing pipeline and logs them for native engine
 * consumption. The C++ native engine reads the {@code index_eval_hints}
 * repeated field from the typed {@code FileSplitDetail} extension.</p>
 *
 * <p>Thread-safe: stateless utility.</p>
 */
public final class NativeIndexHintConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(NativeIndexHintConsumer.class);

    private NativeIndexHintConsumer() {}

    /**
     * Validates and logs index hints present in splits before native dispatch.
     * Returns the total number of hints across all splits.
     *
     * @param splits splits about to be dispatched to native engine
     * @param queryId query identifier for structured logging
     * @param stageId stage identifier for structured logging
     * @return total hint count across all splits
     */
    public static int validateHintsForNativeEngine(List<DataSplit> splits,
                                                    String queryId, int stageId) {
        int totalHints = 0;
        int splitsWithHints = 0;
        for (DataSplit split : splits) {
            int hintCount = DataSplitProtoCodec.fileDetail(split).getIndexEvalHintsCount();
            if (hintCount > 0) {
                totalHints += hintCount;
                splitsWithHints++;
                logHintDetails(split, queryId, stageId);
            }
        }
        if (totalHints > 0) {
            LOG.info("event=native_index_hints_dispatched queryId={} stageId={} "
                            + "splitsWithHints={} totalHints={}",
                    queryId, stageId, splitsWithHints, totalHints);
        }
        return totalHints;
    }

    private static void logHintDetails(DataSplit split, String queryId, int stageId) {
        List<IndexEvalHint> hints = IndexEvalHintProtoCodec.extractHints(split);
        for (IndexEvalHint hint : hints) {
            LOG.debug("event=native_hint_detail queryId={} stageId={} "
                            + "file={} indexId={} indexType={} generation={}",
                    queryId, stageId, split.getOpaqueLocation(),
                    hint.indexId(), hint.indexType(), hint.generation());
        }
    }
}
