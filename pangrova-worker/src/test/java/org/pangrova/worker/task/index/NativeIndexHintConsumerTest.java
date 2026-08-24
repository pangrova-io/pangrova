package org.pangrova.worker.task.index;

import org.pangrova.protocol.v1.ComparableValue;
import org.pangrova.protocol.v1.EqualToPredicate;
import org.pangrova.protocol.v1.IndexEvalHintInfo;
import org.pangrova.protocol.v1.IndexPredicateInfo;
import org.pangrova.protocol.v1.SplitInfo;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link NativeIndexHintConsumer} — verifies hint counting
 * and validation logic for native engine dispatch.
 */
class NativeIndexHintConsumerTest {

    @Test
    void returnsZeroForSplitsWithoutHints() {
        SplitInfo noHints = SplitInfo.newBuilder()
                .setFilePath("data/file1.parquet")
                .build();

        int count = NativeIndexHintConsumer.validateHintsForNativeEngine(
                List.of(noHints), "q-1", 0);
        assertThat(count).isZero();
    }

    @Test
    void countsSingleHint() {
        SplitInfo withHint = SplitInfo.newBuilder()
                .setFilePath("data/file1.parquet")
                .addIndexEvalHints(IndexEvalHintInfo.newBuilder()
                        .setIndexId("idx-1")
                        .setIndexType("BLOOM_FILTER")
                        .setIndexFilePath("/idx/bloom.idx")
                        .setGeneration(1)
                        .setPredicate(IndexPredicateInfo.newBuilder()
                                .setColumn("col")
                                .setEqualTo(EqualToPredicate.newBuilder()
                                        .setValue(ComparableValue.newBuilder()
                                                .setStringValue("hello").build())
                                        .build())
                                .build())
                        .setDataFilePath("data/file1.parquet")
                        .build())
                .build();

        int count = NativeIndexHintConsumer.validateHintsForNativeEngine(
                List.of(withHint), "q-2", 1);
        assertThat(count).isEqualTo(1);
    }

    @Test
    void countsMultipleHintsAcrossSplits() {
        IndexEvalHintInfo hint = IndexEvalHintInfo.newBuilder()
                .setIndexId("idx-1")
                .setIndexType("BLOOM_FILTER")
                .setIndexFilePath("/idx/bloom.idx")
                .setGeneration(1)
                .setPredicate(IndexPredicateInfo.newBuilder()
                        .setColumn("col")
                        .setEqualTo(EqualToPredicate.newBuilder()
                                .setValue(ComparableValue.newBuilder()
                                        .setStringValue("hello").build())
                                .build())
                        .build())
                .setDataFilePath("data/file.parquet")
                .build();

        SplitInfo split1 = SplitInfo.newBuilder()
                .setFilePath("data/file1.parquet")
                .addIndexEvalHints(hint)
                .addIndexEvalHints(hint)
                .build();
        SplitInfo split2 = SplitInfo.newBuilder()
                .setFilePath("data/file2.parquet")
                .addIndexEvalHints(hint)
                .build();
        SplitInfo noHints = SplitInfo.newBuilder()
                .setFilePath("data/file3.parquet")
                .build();

        int count = NativeIndexHintConsumer.validateHintsForNativeEngine(
                List.of(split1, split2, noHints), "q-3", 0);
        assertThat(count).isEqualTo(3);
    }

    @Test
    void handlesEmptySplitList() {
        int count = NativeIndexHintConsumer.validateHintsForNativeEngine(
                List.of(), "q-4", 0);
        assertThat(count).isZero();
    }
}
