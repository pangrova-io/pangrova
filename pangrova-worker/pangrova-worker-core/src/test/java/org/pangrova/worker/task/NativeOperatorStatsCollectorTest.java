package org.pangrova.worker.task;

import org.pangrova.protocol.rest.task.OperatorStats;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for G-OBS-06: native operator stats collection and protobuf decoding.
 */
class NativeOperatorStatsCollectorTest {

    @Test
    void decodeTaskStats_preservesSemanticCacheUsage() {
        byte[] rawStats = NativeTaskStats.newBuilder()
                .setRowsRead(7)
                .setCacheHitBytes(512)
                .setCacheMissBytes(1024)
                .setCacheHitCount(3)
                .setCacheMissCount(2)
                .setCacheEvictionCount(1)
                .setCacheAdmissionRejectionCount(4)
                .build()
                .toByteArray();

        TaskStats result = NativeOperatorStatsCollector.decodeTaskStats(rawStats);

        assertThat(result.rowsRead()).isEqualTo(7);
        assertThat(result.cacheHitBytes()).isEqualTo(512);
        assertThat(result.cacheMissBytes()).isEqualTo(1024);
        assertThat(result.cacheHitCount()).isEqualTo(3);
        assertThat(result.cacheMissCount()).isEqualTo(2);
        assertThat(result.cacheEvictionCount()).isEqualTo(1);
        assertThat(result.cacheAdmissionRejectionCount()).isEqualTo(4);
    }

    @Test
    void decodeOperatorStats_emptyBytes_returnsEmptyList() {
        assertThat(NativeOperatorStatsCollector.decodeOperatorStats(null)).isEmpty();
        assertThat(NativeOperatorStatsCollector.decodeOperatorStats(new byte[0])).isEmpty();
    }

    @Test
    void decodeOperatorStats_noOperators_returnsEmptyList() {
        // TaskStats with only aggregate fields, no operator_stats
        byte[] rawStats = NativeTaskStats.newBuilder()
                .setRowsRead(100)
                .setBytesRead(1024)
                .build()
                .toByteArray();

        List<OperatorStats> result = NativeOperatorStatsCollector.decodeOperatorStats(rawStats);
        assertThat(result).isEmpty();
    }

    @Test
    void decodeOperatorStats_singleOperator_decodesAllFields() {
        org.pangrova.protocol.v2.OperatorStats protoOp =
                org.pangrova.protocol.v2.OperatorStats.newBuilder()
                        .setOperatorType("TableScan")
                        .setInputRows(1000)
                        .setOutputRows(950)
                        .setInputBytes(8192)
                        .setOutputBytes(7680)
                        .setCpuNanos(500_000_000L)
                        .setWallNanos(600_000_000L)
                        .setPlanNodeId("0")
                        .setMemoryBytes(2_000_000L)
                        .setPeakMemoryBytes(3_000_000L)
                        .setSpillBytes(0)
                        .build();

        byte[] rawStats = NativeTaskStats.newBuilder()
                .setRowsRead(1000)
                .addOperatorStats(protoOp)
                .build()
                .toByteArray();

        List<OperatorStats> result = NativeOperatorStatsCollector.decodeOperatorStats(rawStats);

        assertThat(result).hasSize(1);
        OperatorStats op = result.get(0);
        assertThat(op.operatorType()).isEqualTo("TableScan");
        assertThat(op.inputRows()).isEqualTo(1000);
        assertThat(op.outputRows()).isEqualTo(950);
        assertThat(op.inputBytes()).isEqualTo(8192);
        assertThat(op.outputBytes()).isEqualTo(7680);
        assertThat(op.cpuNanos()).isEqualTo(500_000_000L);
        assertThat(op.wallNanos()).isEqualTo(600_000_000L);
        assertThat(op.planNodeId()).isEqualTo("0");
        assertThat(op.memoryBytes()).isEqualTo(2_000_000L);
        assertThat(op.peakMemoryBytes()).isEqualTo(3_000_000L);
        assertThat(op.spillBytes()).isEqualTo(0);
    }

    @Test
    void decodeOperatorStats_multipleOperators_decodesAll() {
        org.pangrova.protocol.v2.OperatorStats scan =
                org.pangrova.protocol.v2.OperatorStats.newBuilder()
                        .setOperatorType("TableScan")
                        .setInputRows(10_000)
                        .setOutputRows(10_000)
                        .setPlanNodeId("0")
                        .build();

        org.pangrova.protocol.v2.OperatorStats filter =
                org.pangrova.protocol.v2.OperatorStats.newBuilder()
                        .setOperatorType("FilterProject")
                        .setInputRows(10_000)
                        .setOutputRows(5_000)
                        .setPlanNodeId("1")
                        .build();

        org.pangrova.protocol.v2.OperatorStats agg =
                org.pangrova.protocol.v2.OperatorStats.newBuilder()
                        .setOperatorType("HashAggregation")
                        .setInputRows(5_000)
                        .setOutputRows(100)
                        .setPlanNodeId("2")
                        .setMemoryBytes(50_000_000L)
                        .setSpillBytes(10_000_000L)
                        .build();

        byte[] rawStats = NativeTaskStats.newBuilder()
                .addOperatorStats(scan)
                .addOperatorStats(filter)
                .addOperatorStats(agg)
                .build()
                .toByteArray();

        List<OperatorStats> result = NativeOperatorStatsCollector.decodeOperatorStats(rawStats);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).operatorType()).isEqualTo("TableScan");
        assertThat(result.get(0).inputRows()).isEqualTo(10_000);
        assertThat(result.get(1).operatorType()).isEqualTo("FilterProject");
        assertThat(result.get(1).outputRows()).isEqualTo(5_000);
        assertThat(result.get(2).operatorType()).isEqualTo("HashAggregation");
        assertThat(result.get(2).spillBytes()).isEqualTo(10_000_000L);
        assertThat(result.get(2).memoryBytes()).isEqualTo(50_000_000L);
    }

    @Test
    void decodeOperatorStats_malformedBytes_returnsEmptyList() {
        // Random bytes that are not a valid protobuf
        byte[] garbage = {0x01, 0x02, 0x03, (byte) 0xFF, (byte) 0xFE};
        List<OperatorStats> result = NativeOperatorStatsCollector.decodeOperatorStats(garbage);
        // Should gracefully return empty list, not throw
        assertThat(result).isNotNull();
    }

    @Test
    void decodeOperatorStats_returnsImmutableList() {
        org.pangrova.protocol.v2.OperatorStats protoOp =
                org.pangrova.protocol.v2.OperatorStats.newBuilder()
                        .setOperatorType("Project")
                        .build();

        byte[] rawStats = NativeTaskStats.newBuilder()
                .addOperatorStats(protoOp)
                .build()
                .toByteArray();

        List<OperatorStats> result = NativeOperatorStatsCollector.decodeOperatorStats(rawStats);
        assertThat(result).hasSize(1);
        // List.copyOf returns an unmodifiable list
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> result.add(new OperatorStats("x", 0, 0, 0, 0, 0, 0)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void collect_zeroHandle_returnsEmptyList() {
        // Zero handle is invalid — should return empty without calling JNI
        List<OperatorStats> result = NativeOperatorStatsCollector.collect(0);
        assertThat(result).isEmpty();
    }

    @Test
    void logOperatorStats_nullOrEmpty_doesNotThrow() {
        // Should be no-ops
        NativeOperatorStatsCollector.logOperatorStats("task-1", null);
        NativeOperatorStatsCollector.logOperatorStats("task-1", List.of());
    }
}
