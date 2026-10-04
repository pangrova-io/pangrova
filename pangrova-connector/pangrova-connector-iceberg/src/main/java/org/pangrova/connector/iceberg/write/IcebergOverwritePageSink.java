package org.pangrova.connector.iceberg.write;

import org.pangrova.common.json.JsonEscaper;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.StringJoiner;

/**
 * Writes pages to an Iceberg table using overwrite semantics.
 *
 * <p>For partitioned tables, uses Iceberg's {@code ReplacePartitions} (dynamic
 * partition overwrite): only partitions touched by the new data are replaced,
 * leaving other partitions intact. For unpartitioned tables, replaces all data.
 */
public final class IcebergOverwritePageSink extends AbstractIcebergPageSink {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergOverwritePageSink.class);

    private final long baseSnapshotId;

    public IcebergOverwritePageSink(IcebergWriteContext ctx) {
        this(ctx, -1L);
    }

    public IcebergOverwritePageSink(IcebergWriteContext ctx,
                                    long baseSnapshotId) {
        super(ctx);
        this.baseSnapshotId = baseSnapshotId;
    }

    @Override
    protected byte[] collectCommitMetadata() {
        validateSnapshotUnchanged();
        if (partitioned) {
            return collectPartitionedMetadata();
        } else {
            return collectUnpartitionedMetadata();
        }
    }

    /**
     * Validates that the table snapshot has not changed since beginOverwrite().
     * Detects concurrent DML that would cause silent data loss.
     */
    private void validateSnapshotUnchanged() {
        if (baseSnapshotId < 0) {
            return;
        }
        org.apache.iceberg.Snapshot current = table.currentSnapshot();
        long currentId = current != null ? current.snapshotId() : -1L;
        if (currentId != baseSnapshotId) {
            throw new IllegalStateException(
                    "Concurrent modification detected on table %s.%s: "
                            .formatted(schemaName, tableName)
                            + "expected snapshot=" + baseSnapshotId
                            + " but found snapshot=" + currentId);
        }
    }

    private byte[] collectUnpartitionedMetadata() {
        try {
            closeUnpartitionedWriterForCommit();
            DataFile dataFile = unpartitionedWriter().toDataFile();
            if (dataFile.recordCount() > 0) {
                LOG.debug("event=overwrite_metadata_collected dataFile={}", dataFile.path());
                return serializeOverwriteMetadata(List.of(dataFile));
            } else {
                LOG.debug("event=overwrite_metadata_collected_empty");
                return serializeOverwriteMetadata(List.of());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to collect overwrite metadata", e);
        }
    }

    private byte[] collectPartitionedMetadata() {
        if (!hasPartitionedData()) {
            LOG.debug("event=partitioned_overwrite_empty");
            return new byte[0];
        }

        List<DataFile> dataFiles = closeAndCollectPartitionFiles();
        LOG.debug("event=partitioned_overwrite_metadata_collected partitions={}", dataFiles.size());
        return serializeOverwriteMetadata(dataFiles);
    }

    /**
     * Serializes overwrite metadata including DataFile descriptors and
     * the base snapshot ID for coordinator-side optimistic concurrency check.
     *
     * <p>Delegates individual DataFile serialization to Iceberg's
     * {@link ContentFileParser} for correct escaping and round-trip fidelity.
     */
    private byte[] serializeOverwriteMetadata(List<DataFile> dataFiles) {
        StringJoiner fileJoiner = new StringJoiner(",", "[", "]");
        for (DataFile df : dataFiles) {
            fileJoiner.add(ContentFileParser.toJson(df, table.spec()));
        }

        var sb = new StringBuilder("{\"operation\":\"OVERWRITE\"");
        sb.append(",\"baseSnapshotId\":").append(baseSnapshotId);
        String partitionSpecJson = org.apache.iceberg.PartitionSpecParser.toJson(table.spec());
        sb.append(",\"partitionSpecJson\":\"")
                .append(JsonEscaper.escape(partitionSpecJson))
                .append("\"");
        sb.append(",\"dataFiles\":").append(fileJoiner);
        sb.append("}");
        return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
