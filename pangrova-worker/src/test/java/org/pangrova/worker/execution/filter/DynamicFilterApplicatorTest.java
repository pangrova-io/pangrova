package org.pangrova.worker.execution.filter;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.protocol.source.DataSplitProtoCodec;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.v2.BloomFilter;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.InjectRuntimeFilterRequest;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.RangeFilter;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.protocol.v2.ValueEncoding;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies typed v2 filters prune only provably irrelevant partitions. */
class DynamicFilterApplicatorTest {

    private final DynamicFilterApplicator applicator = new DynamicFilterApplicator();

    @BeforeEach
    void resetMetrics() {
        DynamicPartitionFilterMetrics.reset();
    }

    @Test
    void bigintRangePrunesOutsidePartitions() {
        List<String> splits = List.of(
                split("s1", "customer_id", "50"),
                split("s2", "customer_id", "150"),
                split("s3", "customer_id", "200"),
                split("s4", "customer_id", "300"));

        var result = applicator.apply(
                splits, List.of(bigintRange("f1", "customer_id", 100, 200)),
                0, "q-range");

        assertThat(result.retainedSplits()).containsExactly(splits.get(1), splits.get(2));
        assertThat(result.totalSplits()).isEqualTo(4);
        assertThat(result.filteredSplits()).isEqualTo(2);
    }

    @Test
    void sbbfPrunesDefiniteMissAndRetainsMissingPartitionMetadata() {
        SplitBlockBloomFilter bloom = SplitBlockBloomFilter.builder(100, 0.001)
                .put("val-A").put("val-B").build();
        String matching = split("s1", "region", "val-A");
        String missing = split("s2", null, null);
        String miss = split("s3", "region", "val-X");

        var result = applicator.apply(
                List.of(matching, missing, miss),
                List.of(utf8Bloom("f2", "region", bloom)), 1, "q-bloom");

        assertThat(result.retainedSplits()).contains(matching, missing).doesNotContain(miss);
    }

    @Test
    void emptyInputsRemainUnchanged() {
        String split = split("s1", "col", "a");
        assertThat(applicator.apply(List.of(split), List.of(), 0, "q").retainedSplits())
                .containsExactly(split);
        assertThat(applicator.apply(List.of(split), null, 0, "q").retainedSplits())
                .containsExactly(split);
        assertThat(applicator.apply(List.of(),
                List.of(bigintRange("f", "id", 1, 2)), 0, "q").retainedSplits())
                .isEmpty();
    }

    @Test
    void unsupportedPredicateFailsClosed() {
        RuntimeFilter unsupported = RuntimeFilter.newBuilder()
                .setColumnName("col")
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH)
                .build();

        assertThatThrownBy(() -> applicator.apply(
                List.of(split("s1", "col", "a")),
                List.of(payload("f", "UNKNOWN", "col", unsupported)), 0, "q"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported dynamic filter predicate");
    }

    private static DynamicFilterPayload bigintRange(
            String id, String column, long lower, long upper) {
        RuntimeFilter filter = RuntimeFilter.newBuilder()
                .setColumnName(column)
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH)
                .setRange(RangeFilter.newBuilder()
                        .setLower(int64(lower)).setUpper(int64(upper))
                        .setLowerInclusive(true).setUpperInclusive(true))
                .build();
        return payload(id, "MIN_MAX", column, filter);
    }

    private static DynamicFilterPayload utf8Bloom(
            String id, String column, SplitBlockBloomFilter bloom) {
        RuntimeFilter filter = RuntimeFilter.newBuilder()
                .setColumnName(column)
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH)
                .setBloom(BloomFilter.newBuilder()
                        .setAlgorithm("SBBF")
                        .setSerializedFilter(ByteString.copyFrom(bloom.toByteArray()))
                        .setProbeType(type("VARCHAR"))
                        .setValueEncoding(ValueEncoding.VALUE_ENCODING_UTF8))
                .build();
        return payload(id, "BLOOM", column, filter);
    }

    private static DynamicFilterPayload payload(
            String id, String type, String column, RuntimeFilter filter) {
        byte[] bytes = InjectRuntimeFilterRequest.newBuilder()
                .setPlanNodeId("node-1")
                .addFilters(filter)
                .build()
                .toByteArray();
        return new DynamicFilterPayload(id, type, bytes, "0", column, 2);
    }

    private static TypedValue int64(long value) {
        return TypedValue.newBuilder().setType(type("BIGINT")).setInt64Value(value).build();
    }

    private static LogicalType type(String sql) {
        return LogicalType.newBuilder().setCanonicalSql(sql).build();
    }

    private static String split(String id, String key, String value) {
        Map<String, String> partitions = key == null ? Map.of() : Map.of(key, value);
        DataSplit split = DataSplit.newBuilder(DataSplitProtoCodec.buildFileSplit(
                new DataSplitProtoCodec.FileSplitInput(
                        "iceberg", "catalog.schema.test", "snapshot", id,
                        "/data/" + id + ".parquet",
                        0, 10, partitions,
                        FileSplitDetail.newBuilder()
                                .setFileFormat(FileFormat.FILE_FORMAT_PARQUET)
                                .build())))
                .setSplitId(id)
                .build();
        return Base64.getEncoder().encodeToString(split.toByteArray());
    }
}
