package org.pangrova.protocol.index;

import org.pangrova.protocol.v1.IndexEvalHintInfo;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.spi.index.eval.IndexEvalHint;
import org.pangrova.spi.index.eval.IndexEvaluatedSplit;
import org.pangrova.spi.index.eval.SplitIndexEvalResult;
import java.util.List;
import java.util.Map;

/**
 * Encodes {@link IndexEvalHint} objects into proto {@link IndexEvalHintInfo}
 * messages and injects them into serialized {@link SplitInfo} bytes.
 *
 * <p>Lives in pangrova-protocol because it bridges pangrova-spi types
 * (IndexEvalHint, IndexPredicate) with proto types (SplitInfo,
 * IndexEvalHintInfo). Called by the coordinator's split serialization path.</p>
 */
public final class IndexEvalHintProtoCodec {

    private IndexEvalHintProtoCodec() {}

    /**
     * Serializes an IndexEvaluatedSplit to proto bytes with eval hints injected.
     *
     * <p>Takes the delegate split's proto bytes, parses as SplitInfo, appends
     * IndexEvalHintInfo messages from the eval result, and re-serializes.</p>
     *
     * @param evaluatedSplit the split with eval hints attached
     * @return serialized SplitInfo proto bytes with hints included
     */
    public static byte[] serializeWithHints(IndexEvaluatedSplit evaluatedSplit) {
        byte[] delegateBytes = evaluatedSplit.unwrap().toSplitInfoProto();
        SplitIndexEvalResult evalResult = evaluatedSplit.evalResult();
        if (!evalResult.hasEvalHints()) {
            return delegateBytes;
        }
        try {
            SplitInfo.Builder builder = SplitInfo.parseFrom(delegateBytes).toBuilder();
            appendHints(builder, evalResult.evalHints());
            return builder.build().toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            // Conservative: return original bytes if parsing fails
            return delegateBytes;
        }
    }

    private static void appendHints(
            SplitInfo.Builder builder,
            Map<String, List<IndexEvalHint>> evalHints) {
        for (var entry : evalHints.entrySet()) {
            String dataFilePath = entry.getKey();
            for (IndexEvalHint hint : entry.getValue()) {
                builder.addIndexEvalHints(toProto(hint, dataFilePath));
            }
        }
    }

    private static IndexEvalHintInfo toProto(IndexEvalHint hint, String dataFilePath) {
        return IndexEvalHintInfo.newBuilder()
                .setIndexId(hint.indexId())
                .setIndexType(hint.indexType().name())
                .setIndexFilePath(hint.indexFilePath())
                .setGeneration(hint.generation())
                .setPredicate(IndexPredicateProtoConverter.toProto(hint.predicate()))
                .setDataFilePath(dataFilePath)
                .build();
    }

    /**
     * Extracts IndexEvalHint objects from a parsed SplitInfo proto.
     *
     * <p>Called by the worker to reconstruct hints from the proto wire format.
     * Returns an empty list if no hints are present.</p>
     *
     * @param splitInfo parsed SplitInfo proto message
     * @return list of reconstructed IndexEvalHint objects
     */
    public static List<IndexEvalHint> extractHints(SplitInfo splitInfo) {
        if (splitInfo.getIndexEvalHintsCount() == 0) {
            return List.of();
        }
        return splitInfo.getIndexEvalHintsList().stream()
                .map(IndexEvalHintProtoCodec::fromProto)
                .toList();
    }

    private static IndexEvalHint fromProto(IndexEvalHintInfo proto) {
        return new IndexEvalHint(
                proto.getIndexId(),
                org.pangrova.spi.index.IndexType.valueOf(proto.getIndexType()),
                IndexPredicateProtoConverter.fromProto(proto.getPredicate()),
                proto.getIndexFilePath(),
                proto.getGeneration());
    }
}
