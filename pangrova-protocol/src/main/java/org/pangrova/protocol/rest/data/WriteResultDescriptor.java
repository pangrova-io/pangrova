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
    @JsonProperty("commit_metadata") byte[] commitMetadata,
    @JsonProperty("commit_fragments") List<byte[]> commitFragments
) {

    private static final int HASH_MULTIPLIER = 31;

    public WriteResultDescriptor {
        dataFiles = dataFiles == null ? List.of() : List.copyOf(dataFiles);
        commitMetadata = commitMetadata != null ? commitMetadata.clone() : new byte[0];
        commitFragments = commitFragments == null
                ? List.of()
                : commitFragments.stream().map(byte[]::clone).toList();
    }

    /** Returns an owned copy so validated metadata cannot be mutated after admission. */
    @Override
    public byte[] commitMetadata() {
        return commitMetadata.clone();
    }

    /** Returns owned copies of every worker-staged commit fragment. */
    @Override
    public List<byte[]> commitFragments() {
        return commitFragments.stream().map(byte[]::clone).toList();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriteResultDescriptor that)) return false;
        return totalRows == that.totalRows
                && totalBytes == that.totalBytes
                && dataFiles.equals(that.dataFiles)
                && java.util.Arrays.equals(commitMetadata, that.commitMetadata)
                && fragmentsEqual(commitFragments, that.commitFragments);
    }

    @Override
    public int hashCode() {
        int result = dataFiles.hashCode();
        result = 31 * result + Long.hashCode(totalRows);
        result = 31 * result + Long.hashCode(totalBytes);
        result = 31 * result + java.util.Arrays.hashCode(commitMetadata);
        result = 31 * result + fragmentsHash(commitFragments);
        return result;
    }

    private static boolean fragmentsEqual(List<byte[]> left, List<byte[]> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int index = 0; index < left.size(); index++) {
            if (!java.util.Arrays.equals(left.get(index), right.get(index))) {
                return false;
            }
        }
        return true;
    }

    private static int fragmentsHash(List<byte[]> fragments) {
        int result = 1;
        for (byte[] fragment : fragments) {
            result = HASH_MULTIPLIER * result + java.util.Arrays.hashCode(fragment);
        }
        return result;
    }
}
