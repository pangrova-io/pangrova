package org.pangrova.execution.split;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.v2.ColumnMapping;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.PhysicalFileScanDetail;
import org.pangrova.protocol.v2.SchemaEvolution;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaDescriptorDeduplicatorTest {

    private static DataSplit split(String id, FileSplitDetail detail) {
        DataSplit base = DataSplit.newBuilder()
                .setSplitId(id)
                .setConnectorId("iceberg")
                .setSourceId("catalog.db.table")
                .setSourceSnapshot("1")
                .setDataObjectId("object-" + id)
                .setOpaqueLocation("s3://bucket/" + id)
                .build();
        return DataSplitProtoCodec.withFileDetail(base, detail);
    }

    private static SchemaEvolution evolution(String target) {
        return SchemaEvolution.newBuilder()
                .addColumnMappings(ColumnMapping.newBuilder()
                        .setSourceName("old")
                        .setTargetName(target)
                        .build())
                .build();
    }

    @Test
    void identicalTableLevelEvolutionsAreStoredOnceAndReferenced() {
        SchemaEvolution shared = evolution("new");
        var splits = List.of(
                split("s1", FileSplitDetail.newBuilder().setSchemaEvolution(shared).build()),
                split("s2", FileSplitDetail.newBuilder().setSchemaEvolution(shared).build()),
                split("s3", FileSplitDetail.newBuilder().setSchemaEvolution(shared).build()));

        var result = SchemaDescriptorDeduplicator.deduplicate(splits);

        assertThat(result.descriptors()).containsExactly(shared);
        for (DataSplit rewritten : result.splits()) {
            FileSplitDetail detail = DataSplitProtoCodec.fileDetail(rewritten);
            assertThat(detail.hasSchemaEvolution()).isFalse();
            assertThat(detail.getSchemaDescriptorRef()).isZero();
        }
    }

    @Test
    void distinctEvolutionsKeepDistinctDescriptorIndexes() {
        var splits = List.of(
                split("s1", FileSplitDetail.newBuilder()
                        .setSchemaEvolution(evolution("a")).build()),
                split("s2", FileSplitDetail.newBuilder()
                        .setSchemaEvolution(evolution("b")).build()));

        var result = SchemaDescriptorDeduplicator.deduplicate(splits);

        assertThat(result.descriptors()).hasSize(2);
        assertThat(DataSplitProtoCodec.fileDetail(result.splits().get(0))
                .getSchemaDescriptorRef()).isZero();
        assertThat(DataSplitProtoCodec.fileDetail(result.splits().get(1))
                .getSchemaDescriptorRef()).isEqualTo(1);
    }

    @Test
    void perFileEvolutionStaysInline() {
        SchemaEvolution perFile = evolution("file-local");
        FileSplitDetail detail = FileSplitDetail.newBuilder()
                .setPhysicalFileScan(PhysicalFileScanDetail.newBuilder()
                        .setPhysicalRows(10)
                        .setSchemaEvolution(perFile))
                .setSchemaEvolution(evolution("table"))
                .build();

        var result = SchemaDescriptorDeduplicator.deduplicate(List.of(split("s1", detail)));

        assertThat(result.descriptors()).isEmpty();
        FileSplitDetail kept = DataSplitProtoCodec.fileDetail(result.splits().get(0));
        assertThat(kept.hasSchemaEvolution()).isTrue();
        assertThat(kept.hasSchemaDescriptorRef()).isFalse();
    }

    @Test
    void emptyEvolutionStaysIdentityMapped() {
        FileSplitDetail detail = FileSplitDetail.newBuilder()
                .setSchemaEvolution(SchemaEvolution.getDefaultInstance())
                .build();

        var result = SchemaDescriptorDeduplicator.deduplicate(List.of(split("s1", detail)));

        assertThat(result.descriptors()).isEmpty();
        FileSplitDetail kept = DataSplitProtoCodec.fileDetail(result.splits().get(0));
        assertThat(kept.hasSchemaEvolution()).isTrue();
        assertThat(kept.hasSchemaDescriptorRef()).isFalse();
    }

    @Test
    void referencedSplitIsSmallerThanInlineSplit() {
        SchemaEvolution wide = SchemaEvolution.newBuilder()
                .addColumnMappings(ColumnMapping.newBuilder()
                        .setSourceName("column_with_a_long_physical_name")
                        .setTargetName("column_with_a_long_logical_name"))
                .build();
        DataSplit inline = split("s1",
                FileSplitDetail.newBuilder().setSchemaEvolution(wide).build());
        DataSplit referenced = SchemaDescriptorDeduplicator
                .deduplicate(List.of(inline))
                .splits()
                .get(0);

        assertThat(referenced.getSerializedSize())
                .isLessThan(inline.getSerializedSize() - 10);
    }
}
