package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/** One exact SDK-owned physical file allocation, made immutable before object creation. */
@Experimental
public record NativePhysicalFileAllocation(
        Location location, byte[] partition, int bucketId, long fileOrdinal,
        long restoredMaxSequence, byte[] restoredFileSetSha256, byte[] taskAttempt) {

    public NativePhysicalFileAllocation {
        Objects.requireNonNull(location, "location");
        partition = Objects.requireNonNull(partition, "partition").clone();
        taskAttempt = Objects.requireNonNull(taskAttempt, "taskAttempt").clone();
        if (partition.length > 64 * 1024 || taskAttempt.length > 64 * 1024
                || bucketId < 0 || fileOrdinal < 0 || restoredMaxSequence < -1) {
            throw new IllegalArgumentException("Invalid physical file allocation");
        }
        restoredFileSetSha256 = NativePhysicalWriteAllocation.copyBytes(
                restoredFileSetSha256, 32, "restored file-set SHA-256");
        if (restoredFileSetSha256.length != 32) throw new IllegalArgumentException("Invalid file-set SHA-256");
    }

    @Override public byte[] partition() { return partition.clone(); }
    @Override public byte[] restoredFileSetSha256() { return restoredFileSetSha256.clone(); }
    @Override public byte[] taskAttempt() { return taskAttempt.clone(); }

    public record Location(String opaqueLocation, String fileName, String fileSource) {
        public Location {
            NativePhysicalWriteAllocation.requireText(opaqueLocation, "physical location");
            NativePhysicalWriteAllocation.requireText(fileName, "file name");
            NativePhysicalWriteAllocation.requireText(fileSource, "file source");
            if (fileName.contains("/") || fileName.contains("\\") || fileName.equals(".") || fileName.equals("..")) {
                throw new IllegalArgumentException("Physical file name must be one path component");
            }
        }
    }
}
