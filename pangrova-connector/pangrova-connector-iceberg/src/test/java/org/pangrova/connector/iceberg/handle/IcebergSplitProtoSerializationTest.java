package org.pangrova.connector.iceberg.handle;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.DeleteFileType;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.IcebergReadMode;
import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.pangrova.spi.connector.metadata.table.TypePromotion;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Contract tests for Iceberg's typed extension inside generic v2 DataSplit. */
class IcebergSplitProtoSerializationTest {

    @Test
    void commonEnvelopeOwnsIdentitySnapshotLocationRangeAndPartitions() throws Exception {
        IcebergSplit split = IcebergSplit.builder(
                        "/data/table/file1.parquet", 17L, 1_024L, "PARQUET")
                .tableName("catalog.schema.table")
                .snapshotId(100L)
                .partitionValues(Map.of("date", "2024-01-01"))
                .build();

        DataSplit data = parse(split);
        FileSplitDetail detail = DataSplitProtoCodec.fileDetail(data);

        assertThat(data.getConnectorId()).isEqualTo("iceberg");
        assertThat(data.getSourceId()).isEqualTo("catalog.schema.table");
        assertThat(data.getSourceSnapshot()).isEqualTo("100");
        assertThat(data.getDataObjectId()).isEqualTo("/data/table/file1.parquet");
        assertThat(data.getOpaqueLocation()).isEqualTo("/data/table/file1.parquet");
        assertThat(data.getStartOffset()).isEqualTo(17L);
        assertThat(data.getLength()).isEqualTo(1_024L);
        assertThat(DataSplitProtoCodec.stringPartitionValues(data))
                .containsEntry("date", "2024-01-01");
        assertThat(detail.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_PARQUET);
    }

    @Test
    void icebergDetailPreservesIncrementalModeAndSnapshotBounds() throws Exception {
        IcebergSplit split = IcebergSplit.builder("/data/file.parquet", 0, 512, "PARQUET")
                .tableName("catalog.schema.table")
                .snapshotId(100).dataSequenceNumber(5).specId(1)
                .readMode("INCREMENTAL").fromSnapshotId(50).toSnapshotId(100)
                .build();

        var iceberg = detail(split).getIceberg();

        assertThat(iceberg.getReadMode())
                .isEqualTo(IcebergReadMode.ICEBERG_READ_MODE_INCREMENTAL);
        assertThat(iceberg.getSnapshotId()).isEqualTo(100);
        assertThat(iceberg.getFromSnapshotId()).isEqualTo(50);
        assertThat(iceberg.getToSnapshotId()).isEqualTo(100);
        assertThat(iceberg.getDataSequenceNumber()).isEqualTo(5);
        assertThat(iceberg.getSpecId()).isEqualTo(1);

    }

    @Test
    void explicitAndCurrentSnapshotSelectionHaveIdenticalCanonicalEvidence() throws Exception {
        var full = IcebergSplit.builder("/data/file.parquet", 17, 512, "PARQUET")
                .tableName("catalog.schema.table").snapshotId(100).dataSequenceNumber(5)
                .specId(1).readMode("FULL").build();
        var historical = IcebergSplit.builder("/data/file.parquet", 17, 512, "PARQUET")
                .tableName("catalog.schema.table").snapshotId(100).dataSequenceNumber(5)
                .specId(1).readMode("TIME_TRAVEL").build();
        assertThat(detail(historical).getIceberg().getReadMode())
                .isEqualTo(IcebergReadMode.ICEBERG_READ_MODE_SNAPSHOT);
        assertThat(historical.toSplitInfoProto()).isEqualTo(full.toSplitInfoProto());
        var differentSnapshot = IcebergSplit.builder("/data/file.parquet", 17, 512, "PARQUET")
                .tableName("catalog.schema.table").snapshotId(101).dataSequenceNumber(5)
                .specId(1).readMode("TIME_TRAVEL").build();
        assertThat(differentSnapshot.toSplitInfoProto()).isNotEqualTo(full.toSplitInfoProto());
    }

    @Test
    void typedDeleteFilesCoverPositionEqualityAndDeletionVector() throws Exception {
        IcebergSplit split = IcebergSplit.builder("/data/file.parquet", 0, 512, "PARQUET")
                .tableName("catalog.schema.table")
                .deleteFiles(List.of(
                        new IcebergDeleteFile(
                                "/delete/pos.parquet", "POSITION", List.of(), 200, 3),
                        new IcebergDeleteFile(
                                "/delete/eq.parquet", "EQUALITY", List.of("id"), 50, 2),
                        new IcebergDeleteFile(
                                "/delete/dv.puffin", "DELETION_VECTOR", List.of(), 0, 4,
                                "/dv/bitmap.bin")))
                .build();

        var deletes = detail(split).getIceberg().getDeleteFilesList();

        assertThat(deletes).extracting(delete -> delete.getDeleteType())
                .containsExactly(
                        DeleteFileType.DELETE_FILE_TYPE_POSITION,
                        DeleteFileType.DELETE_FILE_TYPE_EQUALITY,
                        DeleteFileType.DELETE_FILE_TYPE_DELETION_VECTOR);
        assertThat(deletes.get(1).getEqualityFieldsList()).containsExactly("id");
        assertThat(deletes.get(2).getDeletionVectorLocation()).isEqualTo("/dv/bitmap.bin");
    }

    @Test
    void schemaEvolutionAndResidualPredicateAreTyped() throws Exception {
        IcebergSplit split = IcebergSplit.builder("/data/file.orc", 0, 256, "ORC")
                .tableName("catalog.schema.table")
                .columnMappings(List.of(new ColumnMapping(1, 2, "old_id", "id")))
                .typePromotions(List.of(new TypePromotion(2, "int", "long")))
                .residualPredicate(new byte[]{1, 2, 3})
                .build();

        FileSplitDetail detail = detail(split);

        assertThat(detail.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_ORC);
        assertThat(detail.getSchemaEvolution().getColumnMappings(0).getTargetName())
                .isEqualTo("id");
        assertThat(detail.getSchemaEvolution().getTypePromotions(0).getTargetType())
                .isEqualTo("long");
        assertThat(detail.getSplitConstraint().getPredicate().toByteArray())
                .containsExactly(1, 2, 3);
    }

    @Test
    void generatedV2MessageRoundTripsByteForByte() throws Exception {
        byte[] bytes = IcebergSplit.builder("/data/file.parquet", 1, 2, "PARQUET")
                .tableName("catalog.schema.table")
                .snapshotId(99).build().toSplitInfoProto();
        assertThat(DataSplit.parseFrom(bytes).toByteArray()).isEqualTo(bytes);
    }

    @Test
    void rejectsLanceInsteadOfAdvertisingUnsupportedIcebergIntegration() {
        assertThatThrownBy(() -> IcebergSplit.builder(
                        "/data/vectors.lance", 0, 1, "LANCE")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not expose Lance");
    }

    private static DataSplit parse(IcebergSplit split) throws Exception {
        return DataSplit.parseFrom(split.toSplitInfoProto());
    }

    private static FileSplitDetail detail(IcebergSplit split) throws Exception {
        return DataSplitProtoCodec.fileDetail(parse(split));
    }
}
