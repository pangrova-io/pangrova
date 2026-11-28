package org.pangrova.coordinator.query.execution.split;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.DeletionVectorReference;
import org.pangrova.protocol.v2.FileSplitDetail;
import org.pangrova.protocol.v2.PhysicalFileScanDetail;
import org.pangrova.protocol.v2.ScanPartitionDetail;

import static org.assertj.core.api.Assertions.assertThat;

class SplitPlacementWeightTest {
    private static final long OPEN = 4L * 1024 * 1024;

    @Test
    void manifestPartitionsUseKnownRowsAndRetainANonzeroWeightWithoutThem() {
        var partition = FileSplitDetail.newBuilder()
                .setScanPartition(ScanPartitionDetail.newBuilder().setPhysicalRows(800));
        assertThat(SplitPlacementWeight.estimate(split(partition), 0, 0)).isEqualTo(OPEN + 800 * 32L);

        partition.setScanPartition(ScanPartitionDetail.getDefaultInstance());
        assertThat(SplitPlacementWeight.estimate(split(partition), 0, 0)).isEqualTo(OPEN);
    }

    @Test
    void physicalRowsUseTheExactRowIntervalAndChargeDeletionVectorWork() {
        var scan = PhysicalFileScanDetail.newBuilder().setPhysicalRows(1000).setRowStart(500).setRowEnd(600)
                .setDeletionVector(DeletionVectorReference.newBuilder().setByteLength(250).setCardinality(20));
        var detail = FileSplitDetail.newBuilder().setPhysicalFileScan(scan);
        assertThat(SplitPlacementWeight.estimate(split(detail), 2000, 0))
                .isEqualTo(2 * OPEN + 2000 + 100 * 32L + 250 + 20 * 32L);
    }

    @Test
    void estimatesSaturateWithoutWrappingIntoAnIdlePartition() {
        var detail = FileSplitDetail.newBuilder()
                .setScanPartition(ScanPartitionDetail.newBuilder().setPhysicalRows(Long.MAX_VALUE));
        assertThat(SplitPlacementWeight.estimate(split(detail), Long.MAX_VALUE, 0))
                .isEqualTo(Long.MAX_VALUE);
    }

    private static DataSplit split(FileSplitDetail.Builder detail) {
        return DataSplit.newBuilder().setFileDetail(detail).build();
    }
}
