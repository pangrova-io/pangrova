package org.pangrova.protocol.rest.data;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Result of a write task, containing produced data files and totals.
 */
public record WriteResultDescriptor(
    @JsonProperty("data_files") List<DataFile> dataFiles,
    @JsonProperty("total_rows") long totalRows,
    @JsonProperty("total_bytes") long totalBytes,
    @JsonProperty("commit_metadata") byte[] commitMetadata
) {

    public WriteResultDescriptor {
        dataFiles = dataFiles == null ? List.of() : List.copyOf(dataFiles);
        commitMetadata = commitMetadata != null ? commitMetadata.clone() : new byte[0];
    }

    /** Returns an owned copy so validated metadata cannot be mutated after admission. */
    @Override
    public byte[] commitMetadata() {
        return commitMetadata.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriteResultDescriptor that)) return false;
        return totalRows == that.totalRows
                && totalBytes == that.totalBytes
                && dataFiles.equals(that.dataFiles)
                && java.util.Arrays.equals(commitMetadata, that.commitMetadata);
    }

    @Override
    public int hashCode() {
        int result = dataFiles.hashCode();
        result = 31 * result + Long.hashCode(totalRows);
        result = 31 * result + Long.hashCode(totalBytes);
        result = 31 * result + java.util.Arrays.hashCode(commitMetadata);
        return result;
    }
}
