package org.pangrova.protocol.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;

class DataSplitProtoCodecTest {
    @Test
    void fileSplitSharesImmutableDetailAndTakesAnOrderedPartitionSnapshot() {
        var builder = FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_PARQUET);
        var detail = builder.build();
        var values = new LinkedHashMap<String, String>();
        values.put("z", "old");
        values.put("a", "fixed");
        var input = new DataSplitProtoCodec.FileSplitInput("iceberg", "catalog.schema.table", "125",
                "object", "s3://bucket/file.parquet", 12, 34, values, detail);

        values.put("z", "new");
        builder.setFileFormat(FileFormat.FILE_FORMAT_ORC);

        assertThat(input.detail()).isSameAs(detail);
        assertThatThrownBy(() -> input.partitionValues().put("a", "changed"))
                .isInstanceOf(UnsupportedOperationException.class);
        var split = DataSplitProtoCodec.buildFileSplit(input);
        assertThat(DataSplitProtoCodec.fileDetail(split)).isSameAs(detail);
        assertThat(split.getPartitionValuesList()).satisfiesExactly(
                value -> {
                    assertThat(value.getColumnName()).isEqualTo("z");
                    assertThat(value.getValue().getStringValue()).isEqualTo("old");
                },
                value -> {
                    assertThat(value.getColumnName()).isEqualTo("a");
                    assertThat(value.getValue().getStringValue()).isEqualTo("fixed");
                });
    }

    @Test
    void fileSplitRejectsEmptyOverflowingAndUnsupportedByteIntervals() {
        var parquet = FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_PARQUET).build();
        assertThatThrownBy(() -> fileInput(0, 0, parquet)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fileInput(1, Long.MAX_VALUE, parquet)).isInstanceOf(IllegalArgumentException.class);

        var orc = FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_ORC).build();
        assertThatThrownBy(() -> fileInput(1, 128, orc)).isInstanceOf(IllegalArgumentException.class);
        var dataset = FileSplitDetail.newBuilder().setFileFormat(FileFormat.FILE_FORMAT_LANCE).build();
        assertThat(fileInput(0, 0, dataset).length()).isZero();
    }

    private static DataSplitProtoCodec.FileSplitInput fileInput(long start, long length, FileSplitDetail detail) {
        return new DataSplitProtoCodec.FileSplitInput("source", "lake.schema.table", "1", "object",
                "s3://bucket/file", start, length, Map.of(), detail);
    }

    @Test
    void retiredSplitDetailTagCannotSupplyFileDetail() throws Exception {
        var retiredTag = UnknownFieldSet.Field.newBuilder().addLengthDelimited(
                com.google.protobuf.Any.pack(FileSplitDetail.getDefaultInstance()).toByteString()).build();
        var envelope = DataSplit.newBuilder().setUnknownFields(
                UnknownFieldSet.newBuilder().addField(10, retiredTag).build()).build();
        var parsed = DataSplit.parseFrom(envelope.toByteArray());
        assertThat(parsed.hasFileDetail()).isFalse();
        assertThatThrownBy(() -> DataSplitProtoCodec.fileDetail(parsed))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires file_detail");
    }
}
