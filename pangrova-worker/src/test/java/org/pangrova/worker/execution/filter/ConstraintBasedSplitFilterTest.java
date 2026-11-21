package org.pangrova.worker.execution.filter;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.v2.BloomFilter;
import org.pangrova.protocol.v2.ColumnStatistics;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.InjectRuntimeFilterRequest;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.RangeFilter;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.SplitConstraint;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.protocol.v2.ValueEncoding;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies v2 partition and file-stat constraint pruning. */
class ConstraintBasedSplitFilterTest {

    @Test
    void partitionRangePrunesOnlyProvenMisses() {
        DataSplit us = partitionSplit("s1", "country", "US");
        DataSplit jp = partitionSplit("s2", "country", "JP");
        DataSplit uk = partitionSplit("s3", "country", "UK");

        var result = ConstraintBasedSplitFilter.filter(
                List.of(us, jp, uk), List.of(stringRange("f1", "country", "UA", "US")),
                "q", 0);

        assertThat(result.skippedSplits()).isEqualTo(1);
        assertThat(result.retainedSplits()).extracting(DataSplit::getSplitId)
                .containsExactly("s1", "s3");
    }

    @Test
    void columnStatsPruneNonOverlappingFiles() {
        DataSplit in = statsSplit("s1", "amount", "100", "500");
        DataSplit out = statsSplit("s2", "amount", "600", "900");
        DataSplit overlap = statsSplit("s3", "amount", "400", "700");

        var result = ConstraintBasedSplitFilter.filter(
                List.of(in, out, overlap),
                List.of(bigintRange("f1", "amount", 200, 550)), "q", 0);

        assertThat(result.skippedSplits()).isEqualTo(1);
        assertThat(result.retainedSplits()).extracting(DataSplit::getSplitId)
                .containsExactly("s1", "s3");
    }

    @Test
    void everyRangeMustOverlapWhileBloomIsLeftForRowFiltering() {
        DataSplit split = constrainedSplit("s1", Map.of("year", "2024"),
                List.of(ColumnStatistics.newBuilder()
                        .setColumnName("amount").setMinValue("100").setMaxValue("500")
                        .build()));
        SplitBlockBloomFilter bloom = SplitBlockBloomFilter.builder(10, 0.01)
                .put("2024").build();

        var failed = ConstraintBasedSplitFilter.filter(
                List.of(split), List.of(
                        bigintRange("f1", "year", 2023, 2025),
                        bigintRange("f2", "amount", 600, 900)), "q", 0);
        var ignoredBloom = ConstraintBasedSplitFilter.filter(
                List.of(split), List.of(utf8Bloom("f3", "year", bloom)), "q", 0);

        assertThat(failed.retainedSplits()).isEmpty();
        assertThat(ignoredBloom.retainedSplits()).containsExactly(split);
    }

    @Test
    void noConstraintOrNoFilterRetainsSplit() {
        DataSplit split = fileSplit("s1", Map.of(), SplitConstraint.getDefaultInstance());
        assertThat(ConstraintBasedSplitFilter.filter(
                List.of(split), List.of(bigintRange("f", "x", 1, 2)), "q", 0)
                .retainedSplits()).containsExactly(split);
        assertThat(ConstraintBasedSplitFilter.filter(List.of(split), null, "q", 0)
                .retainedSplits()).containsExactly(split);
    }

    private static DynamicFilterPayload bigintRange(
            String id, String column, long lower, long upper) {
        return payload(id, column, RuntimeFilter.newBuilder()
                .setColumnName(column)
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH)
                .setRange(RangeFilter.newBuilder()
                        .setLower(int64(lower)).setUpper(int64(upper))
                        .setLowerInclusive(true).setUpperInclusive(true))
                .build());
    }

    private static DynamicFilterPayload stringRange(
            String id, String column, String lower, String upper) {
        return payload(id, column, RuntimeFilter.newBuilder()
                .setColumnName(column)
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH)
                .setRange(RangeFilter.newBuilder()
                        .setLower(string(lower)).setUpper(string(upper))
                        .setLowerInclusive(true).setUpperInclusive(true))
                .build());
    }

    private static DynamicFilterPayload utf8Bloom(
            String id, String column, SplitBlockBloomFilter bloom) {
        return payload(id, column, RuntimeFilter.newBuilder()
                .setColumnName(column)
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH)
                .setBloom(BloomFilter.newBuilder()
                        .setAlgorithm("SBBF")
                        .setSerializedFilter(ByteString.copyFrom(bloom.toByteArray()))
                        .setProbeType(type("VARCHAR"))
                        .setValueEncoding(ValueEncoding.VALUE_ENCODING_UTF8))
                .build());
    }

    private static DynamicFilterPayload payload(
            String id, String column, RuntimeFilter filter) {
        return new DynamicFilterPayload(id, filter.hasBloom() ? "BLOOM" : "MIN_MAX",
                InjectRuntimeFilterRequest.newBuilder()
                        .setPlanNodeId("node-1").addFilters(filter).build().toByteArray(),
                "0", column, 2);
    }

    private static DataSplit partitionSplit(String id, String column, String value) {
        return constrainedSplit(id, Map.of(column, value), List.of());
    }

    private static DataSplit statsSplit(
            String id, String column, String min, String max) {
        return constrainedSplit(id, Map.of(), List.of(ColumnStatistics.newBuilder()
                .setColumnName(column).setMinValue(min).setMaxValue(max).build()));
    }

    private static DataSplit constrainedSplit(
            String id, Map<String, String> partitions, List<ColumnStatistics> stats) {
        return fileSplit(id, partitions, SplitConstraint.newBuilder()
                .addAllColumnStatistics(stats).build());
    }

    private static DataSplit fileSplit(
            String id, Map<String, String> partitions, SplitConstraint constraint) {
        FileSplitDetail.Builder detail = FileSplitDetail.newBuilder()
                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET);
        if (!constraint.equals(SplitConstraint.getDefaultInstance())) {
            detail.setSplitConstraint(constraint);
        }
        return DataSplit.newBuilder(DataSplitProtoCodec.buildFileSplit(
                new DataSplitProtoCodec.FileSplitInput(
                        "iceberg", "catalog.schema.test", "snapshot", id,
                        "/data/" + id + ".parquet",
                        0, 10, partitions, detail.build())))
                .setSplitId(id)
                .build();
    }

    private static TypedValue int64(long value) {
        return TypedValue.newBuilder().setType(type("BIGINT")).setInt64Value(value).build();
    }

    private static TypedValue string(String value) {
        return TypedValue.newBuilder().setType(type("VARCHAR")).setStringValue(value).build();
    }

    private static LogicalType type(String sql) {
        return LogicalType.newBuilder().setCanonicalSql(sql).build();
    }
}
