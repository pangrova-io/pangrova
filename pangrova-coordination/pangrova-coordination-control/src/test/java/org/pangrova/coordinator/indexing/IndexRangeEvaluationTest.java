package org.pangrova.coordinator.indexing;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.indexing.api.evaluation.IndexEvaluatedSplit;
import org.pangrova.indexing.api.evaluation.SplitIndexEvalResult;
import org.pangrova.protocol.source.CanonicalConnectorSplit;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.IcebergSplitDetail;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IndexRangeEvaluationTest {
    private static final String SOURCE = "lake.sales.events";

    @Test
    void eachDataObjectAndSnapshotIsEvaluatedOnceAndEveryRangeKeepsItsOwnIdentity() {
        var keptFirst = range("17", "kept", 0);
        var keptSecond = range("17", "kept", 100);
        var prunedFirst = range("17", "pruned", 0);
        var prunedSecond = range("17", "pruned", 100);
        var differentSnapshot = range("18", "kept", 0);
        var decision = new SplitIndexEvalResult(Map.of(), Map.of());
        var kept = IndexAwareSplitFetcher.filterByDataObject(
                List.of(keptFirst, prunedFirst, keptSecond, prunedSecond, differentSnapshot),
                SOURCE, representatives -> {
                    assertThat(representatives).containsExactly(keptFirst, prunedFirst, differentSnapshot);
                    return List.of(new IndexEvaluatedSplit(keptFirst, decision), differentSnapshot);
                });

        assertThat(kept).hasSize(3);
        assertThat(kept.subList(0, 2)).allSatisfy(split -> assertThat(((IndexEvaluatedSplit) split).evalResult())
                .isSameAs(decision)).extracting(split -> ((IndexEvaluatedSplit) split).unwrap())
                .containsExactly(keptFirst, keptSecond);
        assertThat(kept.get(2)).isSameAs(differentSnapshot);
    }

    private static ConnectorSplit range(String snapshot, String file, long start) {
        return (CanonicalConnectorSplit) () -> DataSplit.newBuilder().setSourceId(SOURCE)
                .setSourceSnapshot(snapshot).setDataObjectId(file).setStartOffset(start).setLength(100)
                .setFileDetail(FileSplitDetail.newBuilder().setIceberg(IcebergSplitDetail.getDefaultInstance()))
                .build();
    }
}
