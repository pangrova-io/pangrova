package org.pangrova.connector.paimon.write;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.api.metadata.NativePhysicalFileReceipt;

import java.util.List;

/** SDK metadata publication only; never constructs or executes a business-row writer. */
final class PaimonPhysicalCommitMessages {
    private PaimonPhysicalCommitMessages() {}

    /** Builds SDK commit messages from the verified physical append receipts. */
    static List<CommitMessage> append(PaimonOutputTableHandle handle, List<NativePhysicalFileReceipt> receipts) {
        int buckets = handle.nativePhysicalWriteAllocation().orElseThrow().policy().bucketCount();
        return receipts.stream().map(receipt -> message(receipt, buckets)).toList();
    }

    private static CommitMessage message(NativePhysicalFileReceipt receipt, int buckets) {
        var metrics = receipt.metrics();
        // Pinned SDK LanceFileFormat supplies no StatsExtractor. Its real append writer consequently
        // publishes EMPTY_STATS; optional Native observations must not fabricate SDK FULL bounds.
        var file = DataFileMeta.forAppend(receipt.allocation().location().fileName(), metrics.sizeBytes(),
                metrics.physicalRows(), SimpleStats.EMPTY_STATS, metrics.minSequence(), metrics.maxSequence(),
                metrics.schemaId(), List.of(), null, FileSource.APPEND, null, null, null, null);
        return new CommitMessageImpl(BinaryRow.EMPTY_ROW, receipt.allocation().bucketId(), buckets,
                new DataIncrement(List.of(file), List.of(), List.of()), CompactIncrement.emptyIncrement());
    }
}
