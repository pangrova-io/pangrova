package org.pangrova.coordinator.query.execution.split;

import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.PaimonSplitDetail;
import org.pangrova.protocol.v2.PhysicalFileScanDetail;

/** Byte-equivalent scan work estimated exclusively from already-bound planning metadata. */
public final class SplitPlacementWeight {
    // Keep the open cost independently of the payload size. Spark uses the same initial unit.
    private static final long FILE_OPEN_BYTES = 4L * 1024 * 1024;
    private static final long DECODE_BYTES_PER_ROW = 32L;
    private static final long MERGE_BYTES_PER_ROW = 64L;

    private SplitPlacementWeight() {}

    /** Estimates a restored split from its immutable physical range and typed read detail. */
    public static long forBoundSplit(DataSplit split) {
        return estimate(split, Math.max(0L, split.getLength()), 0L);
    }

    static long estimate(DataSplit data, long scanBytes, long estimatedRows) {
        if (!data.hasFileDetail()) {
            return Math.max(1L, scanBytes);
        }
        var detail = data.getFileDetail();
        if (detail.hasScanPartition()) {
            // The manifest partition exposes rows but does not expose its physical file count.
            return add(Math.max(FILE_OPEN_BYTES, scanBytes),
                    rowWork(detail.getScanPartition().getPhysicalRows(), DECODE_BYTES_PER_ROW));
        }
        if (detail.hasPaimon() && detail.getPaimon().getNeedsMerge()) {
            return mergeWork(scanBytes, detail.getPaimon());
        }
        long work = add(scanBytes, FILE_OPEN_BYTES);
        if (detail.hasPhysicalFileScan()) {
            return add(work, physicalFileWork(detail.getPhysicalFileScan(), DECODE_BYTES_PER_ROW));
        }
        work = add(work, rowWork(estimatedRows, DECODE_BYTES_PER_ROW));
        if (detail.hasIceberg()) {
            for (var delete : detail.getIceberg().getDeleteFilesList()) {
                // The connector estimate already includes the encoded delete bytes.
                work = add(work, add(FILE_OPEN_BYTES, rowWork(delete.getRecordCount(), DECODE_BYTES_PER_ROW)));
            }
        }
        return work;
    }

    private static long mergeWork(long scanBytes, PaimonSplitDetail paimon) {
        long work = scanBytes;
        for (var level : paimon.getLsmLevelsList()) {
            for (var file : level.getFilesList()) {
                work = add(work, add(FILE_OPEN_BYTES, physicalFileWork(file.getScan(), MERGE_BYTES_PER_ROW)));
            }
        }
        return Math.max(FILE_OPEN_BYTES, work);
    }

    private static long physicalFileWork(PhysicalFileScanDetail file, long bytesPerRow) {
        long work = file.getRowStart() >= 0 && file.getRowEnd() >= file.getRowStart()
                ? rowWork(file.getRowEnd() - file.getRowStart(), bytesPerRow) : 0L;
        if (file.hasDeletionVector()) {
            var deletion = file.getDeletionVector();
            work = add(work, add(FILE_OPEN_BYTES, Math.max(0L, deletion.getByteLength())));
            if (deletion.hasCardinality()) {
                work = add(work, rowWork(deletion.getCardinality(), DECODE_BYTES_PER_ROW));
            }
        }
        return work;
    }

    private static long rowWork(long rows, long bytesPerRow) {
        return Math.min(Long.MAX_VALUE / bytesPerRow, Math.max(0L, rows)) * bytesPerRow;
    }

    private static long add(long left, long right) {
        return left + Math.min(Long.MAX_VALUE - left, right);
    }
}
