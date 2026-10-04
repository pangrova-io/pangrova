package org.pangrova.connector.iceberg.write;

import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.StringJoiner;

/**
 * Writes pages of data to an Iceberg table using append semantics.
 *
 * <p>For unpartitioned tables, writes a single data file. For partitioned
 * tables, buffers records and writes one data file per partition so that
 * each file carries correct partition metadata for pruning and overwrite.
 */
public class IcebergPageSink extends AbstractIcebergPageSink {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergPageSink.class);

    public IcebergPageSink(IcebergWriteContext ctx) {
        super(ctx);
    }

    @Override
    protected byte[] collectCommitMetadata() {
        if (partitioned) {
            return collectPartitionedMetadata();
        } else {
            return collectUnpartitionedMetadata();
        }
    }

    private byte[] collectUnpartitionedMetadata() {
        try {
            closeUnpartitionedWriterForCommit();
            DataFile dataFile = unpartitionedWriter().toDataFile();
            if (dataFile.recordCount() == 0) {
                // Closing an unused Parquet writer can yield a descriptor for
                // a path that was never created. An empty append is a no-op;
                // never publish that descriptor for the coordinator to commit.
                LOG.debug("event=unpartitioned_append_empty");
                return new byte[0];
            }
            LOG.debug("event=append_metadata_collected dataFile={}", dataFile.path());
            return serializeDataFiles(List.of(dataFile));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close Parquet writer", e);
        }
    }

    private byte[] collectPartitionedMetadata() {
        if (!hasPartitionedData()) {
            LOG.debug("event=partitioned_append_empty");
            return new byte[0];
        }

        List<DataFile> dataFiles = closeAndCollectPartitionFiles();
        LOG.debug("event=partitioned_append_metadata_collected partitions={}", dataFiles.size());
        return serializeDataFiles(dataFiles);
    }

    /**
     * Serializes DataFile descriptors as a JSON array for coordinator-side commit.
     * Delegates to Iceberg's {@link ContentFileParser} for correct escaping and
     * round-trip fidelity.
     */
    private byte[] serializeDataFiles(List<DataFile> dataFiles) {
        StringJoiner joiner = new StringJoiner(",", "[", "]");
        for (DataFile df : dataFiles) {
            joiner.add(ContentFileParser.toJson(df, table.spec()));
        }
        return joiner.toString().getBytes(StandardCharsets.UTF_8);
    }
}
