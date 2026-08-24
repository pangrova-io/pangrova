package org.pangrova.connector.iceberg.handle;

import java.util.List;
import java.util.Map;

import org.pangrova.protocol.v1.DeleteFileInfo;
import org.pangrova.protocol.v1.DeleteFileType;
import org.pangrova.protocol.v1.FileFormat;
import org.pangrova.protocol.v1.IcebergReadMode;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.pangrova.spi.connector.metadata.table.TypePromotion;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for IcebergSplit.toSplitInfoProto() serialization.
 * Validates: Requirements R14.4, R19.2
 */
class IcebergSplitProtoSerializationTest {

    @Test
    void serializesBasicFieldsCorrectly() throws Exception {
        IcebergSplit split = IcebergSplit.builder(
                "/data/table/file1.parquet", 0L, 1024L, "PARQUET")
                .partitionValues(Map.of("date", "2024-01-01"))
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getConnectorId()).isEqualTo("iceberg");
        assertThat(info.getFilePath()).isEqualTo("/data/table/file1.parquet");
        assertThat(info.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_PARQUET);
        assertThat(info.getStartOffset()).isZero();
        assertThat(info.getLength()).isEqualTo(1024L);
        assertThat(info.getPartitionValuesMap()).containsEntry("date", "2024-01-01");
        assertThat(info.getTableName()).isEmpty();
    }

    @Test
    void serializesIcebergSplitInfoFields() throws Exception {
        IcebergSplit split = IcebergSplit.builder(
                "/data/file.parquet", 0L, 512L, "PARQUET")
                .snapshotId(100L)
                .dataSequenceNumber(5L)
                .specId(1)
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.hasIcebergInfo()).isTrue();
        assertThat(info.getIcebergInfo().getReadMode()).isEqualTo(IcebergReadMode.ICEBERG_READ_SNAPSHOT);
        assertThat(info.getIcebergInfo().getSnapshotId()).isEqualTo(100L);
        assertThat(info.getIcebergInfo().getDataSequenceNumber()).isEqualTo(5L);
        assertThat(info.getIcebergInfo().getSpecId()).isEqualTo(1);
        assertThat(info.getIcebergInfo().getContent()).isZero();
    }

    @Test
    void serializesIncrementalReadMode() throws Exception {
        IcebergSplit split = IcebergSplit.builder(
                "/data/file.parquet", 0L, 512L, "PARQUET")
                .snapshotId(100L)
                .dataSequenceNumber(5L)
                .specId(1)
                .readMode("INCREMENTAL")
                .fromSnapshotId(50L)
                .toSnapshotId(100L)
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getIcebergInfo().getReadMode()).isEqualTo(IcebergReadMode.ICEBERG_READ_INCREMENTAL);
        assertThat(info.getIcebergInfo().getFromSnapshotId()).isEqualTo(50L);
        assertThat(info.getIcebergInfo().getToSnapshotId()).isEqualTo(100L);
    }

    /**
     * Validates: Requirements R7.3, R7.4
     */
    @Test
    void serializesTimeTravelReadMode() throws Exception {
        IcebergSplit split = IcebergSplit.builder(
                "/data/file.parquet", 0L, 512L, "PARQUET")
                .partitionValues(Map.of("date", "2024-06-15"))
                .snapshotId(777L)
                .dataSequenceNumber(5L)
                .specId(1)
                .readMode("TIME_TRAVEL")
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getIcebergInfo().getReadMode()).isEqualTo(IcebergReadMode.ICEBERG_READ_TIME_TRAVEL);
        assertThat(info.getIcebergInfo().getSnapshotId()).isEqualTo(777L);
        assertThat(info.getIcebergInfo().getFromSnapshotId()).isZero();
        assertThat(info.getIcebergInfo().getToSnapshotId()).isZero();
        assertThat(info.getPartitionValuesMap()).containsEntry("date", "2024-06-15");
    }

    @Test
    void serializesOrcFileFormat() throws Exception {
        IcebergSplit split = IcebergSplit.builder(
                "/data/file.orc", 0L, 256L, "ORC")
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_ORC);
    }

    @Test
    void serializesPositionDeleteFiles() throws Exception {
        IcebergDeleteFile posDelete = new IcebergDeleteFile(
                "/delete/pos.parquet", "POSITION", List.of(), 100L, 3L);

        IcebergSplit split = IcebergSplit.builder(
                "/data/file.parquet", 0L, 512L, "PARQUET")
                .snapshotId(100L)
                .dataSequenceNumber(5L)
                .specId(1)
                .deleteFiles(List.of(posDelete))
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getIcebergInfo().getDeleteFilesCount()).isEqualTo(1);
        DeleteFileInfo df = info.getIcebergInfo().getDeleteFiles(0);
        assertThat(df.getFilePath()).isEqualTo("/delete/pos.parquet");
        assertThat(df.getDeleteType()).isEqualTo(DeleteFileType.DELETE_FILE_TYPE_POSITION);
        assertThat(df.getRecordCount()).isEqualTo(100L);
        assertThat(df.getDataSequenceNumber()).isEqualTo(3L);
        assertThat(df.getEqualityFieldsList()).isEmpty();
        assertThat(df.getDvFilePath()).isEmpty();
    }

    @Test
    void serializesEqualityDeleteFiles() throws Exception {
        IcebergDeleteFile eqDelete = new IcebergDeleteFile(
                "/delete/eq.parquet", "EQUALITY", List.of("id", "name"), 50L, 2L);

        IcebergSplit split = IcebergSplit.builder(
                "/data/file.parquet", 0L, 512L, "PARQUET")
                .snapshotId(100L)
                .dataSequenceNumber(5L)
                .specId(1)
                .deleteFiles(List.of(eqDelete))
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        DeleteFileInfo df = info.getIcebergInfo().getDeleteFiles(0);
        assertThat(df.getDeleteType()).isEqualTo(DeleteFileType.DELETE_FILE_TYPE_EQUALITY);
        assertThat(df.getEqualityFieldsList()).containsExactly("id", "name");
    }

    @Test
    void serializesDeletionVectorDeleteFiles() throws Exception {
        IcebergDeleteFile dvDelete = new IcebergDeleteFile(
                "/delete/dv.puffin", "DELETION_VECTOR", List.of(), 0L, 4L,
                "/dv/bitmap-001.bin");

        IcebergSplit split = IcebergSplit.builder(
                "/data/file.parquet", 0L, 512L, "PARQUET")
                .snapshotId(100L)
                .dataSequenceNumber(5L)
                .specId(1)
                .deleteFiles(List.of(dvDelete))
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        DeleteFileInfo df = info.getIcebergInfo().getDeleteFiles(0);
        assertThat(df.getDeleteType()).isEqualTo(DeleteFileType.DELETE_FILE_TYPE_DELETION_VECTOR);
        assertThat(df.getDvFilePath()).isEqualTo("/dv/bitmap-001.bin");
    }

    @Test
    void serializesSchemaEvolution() throws Exception {
        List<ColumnMapping> mappings = List.of(
                new ColumnMapping(1, 1, "old_id", "id"),
                new ColumnMapping(2, 3, "old_name", "full_name"));
        List<TypePromotion> promotions = List.of(
                new TypePromotion(1, "int", "long"),
                new TypePromotion(4, "float", "double"));

        IcebergSplit split = new IcebergSplit(
                "test_schema.test_table",
                "/data/file.parquet", 0L, 512L, 0L, "PARQUET",
                Map.of(),
                100L, 5L, 1, "DATA", "FULL", 0L, 0L, List.of(),
                mappings, promotions, new byte[0]);

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getTableName()).isEqualTo("test_schema.test_table");
        assertThat(info.hasSchemaEvolution()).isTrue();
        assertThat(info.getSchemaEvolution().getColumnMappingsCount()).isEqualTo(2);
        assertThat(info.getSchemaEvolution().getColumnMappings(0).getSourceName()).isEqualTo("old_id");
        assertThat(info.getSchemaEvolution().getColumnMappings(0).getTargetName()).isEqualTo("id");
        assertThat(info.getSchemaEvolution().getColumnMappings(1).getSourceFieldId()).isEqualTo(2);
        assertThat(info.getSchemaEvolution().getColumnMappings(1).getTargetFieldId()).isEqualTo(3);

        assertThat(info.getSchemaEvolution().getTypePromotionsCount()).isEqualTo(2);
        assertThat(info.getSchemaEvolution().getTypePromotions(0).getFieldId()).isEqualTo(1);
        assertThat(info.getSchemaEvolution().getTypePromotions(0).getSourceType()).isEqualTo("int");
        assertThat(info.getSchemaEvolution().getTypePromotions(0).getTargetType()).isEqualTo("long");
    }

    @Test
    void omitsSchemaEvolutionWhenEmpty() throws Exception {
        IcebergSplit split = IcebergSplit.builder(
                "/data/file.parquet", 0L, 512L, "PARQUET")
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.hasSchemaEvolution()).isFalse();
    }

    @Test
    void serializesMultipleDeleteFileTypes() throws Exception {
        List<IcebergDeleteFile> deleteFiles = List.of(
                new IcebergDeleteFile("/delete/dv.puffin", "DELETION_VECTOR", List.of(), 0L, 4L, "/dv/bm.bin"),
                new IcebergDeleteFile("/delete/pos.parquet", "POSITION", List.of(), 200L, 3L),
                new IcebergDeleteFile("/delete/eq.parquet", "EQUALITY", List.of("pk"), 50L, 2L));

        IcebergSplit split = IcebergSplit.builder(
                "/data/file.parquet", 0L, 512L, "PARQUET")
                .snapshotId(100L)
                .dataSequenceNumber(5L)
                .specId(1)
                .deleteFiles(deleteFiles)
                .build();

        SplitInfo info = SplitInfo.parseFrom(split.toSplitInfoProto());

        assertThat(info.getIcebergInfo().getDeleteFilesCount()).isEqualTo(3);
        assertThat(info.getIcebergInfo().getDeleteFiles(0).getDeleteType())
                .isEqualTo(DeleteFileType.DELETE_FILE_TYPE_DELETION_VECTOR);
        assertThat(info.getIcebergInfo().getDeleteFiles(1).getDeleteType())
                .isEqualTo(DeleteFileType.DELETE_FILE_TYPE_POSITION);
        assertThat(info.getIcebergInfo().getDeleteFiles(2).getDeleteType())
                .isEqualTo(DeleteFileType.DELETE_FILE_TYPE_EQUALITY);
    }

    @Test
    void protoRoundTripPreservesAllFields() throws Exception {
        List<ColumnMapping> mappings = List.of(new ColumnMapping(1, 2, "src", "tgt"));
        List<TypePromotion> promotions = List.of(new TypePromotion(1, "int", "long"));
        IcebergDeleteFile dvDelete = new IcebergDeleteFile(
                "/delete/dv.puffin", "DELETION_VECTOR", List.of(), 0L, 4L, "/dv/bm.bin");

        IcebergSplit split = new IcebergSplit(
                "test_schema.test_table",
                "/data/file.parquet", 100L, 2048L, 0L, "PARQUET",
                Map.of("year", "2024", "month", "03"),
                999L, 7L, 2, "DATA", "TIME_TRAVEL", 0L, 0L,
                List.of(dvDelete), mappings, promotions, new byte[0]);

        byte[] bytes = split.toSplitInfoProto();
        SplitInfo info = SplitInfo.parseFrom(bytes);

        // Re-serialize and verify byte-level equality
        byte[] roundTripped = info.toByteArray();
        assertThat(roundTripped).isEqualTo(bytes);
    }
}
