package org.pangrova.worker.task.index;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.ComparableValue;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.EqualToPredicate;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.IndexEvalHintDetail;
import org.pangrova.protocol.v2.IndexPredicate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests typed v2 index hints at the final Worker-to-Native boundary. */
class NativeIndexHintConsumerTest {

    @Test
    void countsHintsAcrossSplits() {
        IndexEvalHintDetail hint = IndexEvalHintDetail.newBuilder()
                .setIndexId("idx-1")
                .setIndexType("BLOOM_FILTER")
                .setIndexFilePath("/idx/bloom.idx")
                .setGeneration(1)
                .setDataFilePath("/data/file.parquet")
                .setPredicate(IndexPredicate.newBuilder()
                        .setColumn("col")
                        .setEqualTo(EqualToPredicate.newBuilder()
                                .setValue(ComparableValue.newBuilder()
                                        .setStringValue("hello"))))
                .build();

        assertThat(NativeIndexHintConsumer.validateHintsForNativeEngine(
                List.of(split(List.of(hint, hint)), split(List.of(hint)), split(List.of())),
                "q-1", 0)).isEqualTo(3);
    }

    @Test
    void emptyAndHintlessSplitsReturnZero() {
        assertThat(NativeIndexHintConsumer.validateHintsForNativeEngine(
                List.of(), "q-2", 0)).isZero();
        assertThat(NativeIndexHintConsumer.validateHintsForNativeEngine(
                List.of(split(List.of())), "q-2", 0)).isZero();
    }

    private static DataSplit split(List<IndexEvalHintDetail> hints) {
        return DataSplitProtoCodec.buildFileSplit(new DataSplitProtoCodec.FileSplitInput(
                "files", "catalog.schema.test", "snapshot", "object",
                "/data/file.parquet", 0, 10, Map.of(),
                FileSplitDetail.newBuilder()
                        .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                        .addAllIndexEvalHints(hints)
                        .build()));
    }
}
