package org.pangrova.execution.split;


import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanonicalSplitValidatorTest {

    @Test
    void acceptsOneCompleteCanonicalSplit() {
        assertThatCode(() -> CanonicalSplitValidator.validateAll(List.of(split("split-1")), "query-1", 2))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsDuplicateSplitIdentityBeforeDispatch() {
        DataSplit split = split("split-1");

        assertThatThrownBy(() -> CanonicalSplitValidator.validateAll(List.of(split, split), "query-1", 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("queryId=query-1 stageId=2");
    }

    @Test
    void rejectsAFileSplitWithNoCanonicalDetail() {
        var incomplete = split("split-1").toBuilder().clearFileDetail().build();
        assertThatThrownBy(() -> CanonicalSplitValidator.validateAll(List.of(incomplete), "query-1", 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("queryId=query-1 stageId=2")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    private static DataSplit split(String id) {
        return DataSplit.newBuilder()
                .setSplitId(id)
                .setConnectorId("iceberg")
                .setSourceId("catalog.schema.table")
                .setSourceSnapshot("snapshot-1")
                .setDataObjectId("object-1")
                .setOpaqueLocation("s3://bucket/file.parquet")
                .setFileDetail(FileSplitDetail.getDefaultInstance())
                .build();
    }
}
