package org.pangrova.coordinator.query.execution.dispatch.dag.split;

import java.util.Base64;
import java.util.List;

import com.google.protobuf.Any;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanonicalSplitParserTest {

    @Test
    void acceptsOneCompleteCanonicalSplit() {
        String encoded = encode(split("split-1"));

        assertThat(CanonicalSplitParser.parseAll(List.of(encoded), "query-1", 2))
                .containsExactly(split("split-1"));
    }

    @Test
    void rejectsDuplicateSplitIdentityBeforeDispatch() {
        String encoded = encode(split("split-1"));

        assertThatThrownBy(() -> CanonicalSplitParser.parseAll(
                List.of(encoded, encoded), "query-1", 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("queryId=query-1 stageId=2");
    }

    private static String encode(DataSplit split) {
        return Base64.getEncoder().encodeToString(split.toByteArray());
    }

    private static DataSplit split(String id) {
        return DataSplit.newBuilder()
                .setSplitId(id)
                .setConnectorId("iceberg")
                .setSourceId("catalog.schema.table")
                .setSourceSnapshot("snapshot-1")
                .setDataObjectId("object-1")
                .setOpaqueLocation("s3://bucket/file.parquet")
                .setConnectorDetail(Any.pack(FileSplitDetail.getDefaultInstance()))
                .build();
    }
}
