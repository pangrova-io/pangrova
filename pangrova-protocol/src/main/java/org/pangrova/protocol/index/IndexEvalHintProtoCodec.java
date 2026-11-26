package org.pangrova.protocol.index;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.IndexEvalHintDetail;
import org.pangrova.indexing.api.evaluation.IndexEvalHint;
import org.pangrova.indexing.api.evaluation.SplitIndexEvalResult;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Encodes {@link IndexEvalHint} objects into typed v2 file split details.
 *
 * <p>Lives in pangrova-protocol because it bridges connector/indexing SPI types
 * (IndexEvalHint, IndexPredicate) with the canonical DataSplit and IndexEvalHintDetail.</p>
 */
public final class IndexEvalHintProtoCodec {

    private IndexEvalHintProtoCodec() {}

    /** Preserves the canonical split identity and attaches the applicable index hints. */
    public static DataSplit withHints(DataSplit split, SplitIndexEvalResult evalResult) {
        if (!evalResult.hasEvalHints()) return split;
        FileSplitDetail.Builder detail = DataSplitProtoCodec.fileDetail(split).toBuilder();
        appendHints(detail, evalResult.evalHints());
        return DataSplitProtoCodec.withFileDetail(split, detail.build());
    }

    private static void appendHints(
            FileSplitDetail.Builder builder,
            Map<String, List<IndexEvalHint>> evalHints) {
        for (var entry : evalHints.entrySet()) {
            String dataFilePath = entry.getKey();
            for (IndexEvalHint hint : entry.getValue()) {
                builder.addIndexEvalHints(toProto(hint, dataFilePath));
            }
        }
    }

    private static IndexEvalHintDetail toProto(IndexEvalHint hint, String dataFilePath) {
        return IndexEvalHintDetail.newBuilder()
                .setIndexId(hint.indexId())
                .setIndexType(hint.indexType().name())
                .setIndexFilePath(hint.indexFilePath())
                .setGeneration(hint.generation())
                .setIndexSizeBytes(hint.indexSizeBytes())
                .setIndexSha256(ByteString.copyFrom(HexFormat.of().parseHex(hint.checksum())))
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
    public static List<IndexEvalHint> extractHints(DataSplit split) {
        FileSplitDetail detail = DataSplitProtoCodec.fileDetail(split);
        if (detail.getIndexEvalHintsCount() == 0) {
            return List.of();
        }
        return detail.getIndexEvalHintsList().stream()
                .map(IndexEvalHintProtoCodec::fromProto)
                .toList();
    }

    private static IndexEvalHint fromProto(IndexEvalHintDetail proto) {
        return new IndexEvalHint(
                proto.getIndexId(),
                org.pangrova.indexing.api.enums.IndexType.valueOf(proto.getIndexType()),
                IndexPredicateProtoConverter.fromProto(proto.getPredicate()),
                proto.getIndexFilePath(),
                proto.getGeneration(),
                proto.getIndexSizeBytes(),
                HexFormat.of().formatHex(proto.getIndexSha256().toByteArray()));
    }
}
