package org.pangrova.connector.paimon.write;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.spi.metadata.NativePhysicalFileReceipt;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/** SDK metadata publication only; never constructs or executes a business-row writer. */
public final class PaimonPhysicalCommitMessages {
    private PaimonPhysicalCommitMessages() {}

    /**
     * Encodes verified physical receipts for the SDK append publication.
     *
     * @param handle the bound append output
     * @param receipts the winning closed-file receipts
     * @return SDK commit metadata without business row payloads
     */
    public static byte[] append(PaimonOutputTableHandle handle, List<NativePhysicalFileReceipt> receipts) {
        int buckets = handle.nativePhysicalWriteAllocation().orElseThrow().policy().bucketCount();
        List<CommitMessage> messages = receipts.stream().map(receipt -> message(receipt, buckets)).toList();
        var serializer = new CommitMessageSerializer();
        try (var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes)) {
            out.writeInt(serializer.getVersion());
            out.writeUTF("APPEND");
            out.writeUTF(handle.schemaName());
            out.writeUTF(handle.tableName());
            out.writeInt(messages.size());
            for (var message : messages) {
                byte[] encoded = serializer.serialize(message);
                out.writeInt(encoded.length);
                out.write(encoded);
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException failure) {
            throw new IllegalStateException("SDK physical append commit metadata serialization failed", failure);
        }
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
