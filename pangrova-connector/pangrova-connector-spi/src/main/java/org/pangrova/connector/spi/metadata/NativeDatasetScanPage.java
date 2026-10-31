package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.util.List;
import java.util.OptionalLong;

/** Bounded metadata descriptors from one exact Native manifest, never dataset rows. */
@Experimental
public record NativeDatasetScanPage(long version, byte[] manifestSha256, byte[] schemaFingerprint,
        byte[] serializedCatalogBinding, long totalPartitions, List<Partition> partitions, OptionalLong nextAfter) {
    /** Format-owned physical numeric ID scoped to the exact dataset version. */
    @Experimental
    public record Partition(long id, long physicalRows) {
        public Partition {
            if (id < 0 || physicalRows < 0) throw new IllegalArgumentException("Invalid scan partition bounds");
        }
    }

    public NativeDatasetScanPage {
        requireDigests(manifestSha256, schemaFingerprint);
        if (version <= 0 || totalPartitions < 0
                || serializedCatalogBinding.length == 0 || partitions.size() > 256 || nextAfter == null) {
            throw new IllegalArgumentException("Invalid immutable scan page");
        }
        manifestSha256 = manifestSha256.clone();
        schemaFingerprint = schemaFingerprint.clone();
        serializedCatalogBinding = serializedCatalogBinding.clone();
        partitions = List.copyOf(partitions);
        requireContinuation(partitions, nextAfter);
    }

    private static void requireContinuation(List<Partition> partitions, OptionalLong nextAfter) {
        if (nextAfter.isPresent() && (partitions.isEmpty()
                || nextAfter.getAsLong() != partitions.get(partitions.size() - 1).id())) {
            throw new IllegalArgumentException("Scan continuation does not match the final partition");
        }
    }
    private static void requireDigests(byte[] manifest, byte[] schema) {
        if (manifest.length != 32 || schema.length != 32) throw new IllegalArgumentException("Invalid scan digests");
    }
    @Override public byte[] manifestSha256() { return manifestSha256.clone(); }
    @Override public byte[] schemaFingerprint() { return schemaFingerprint.clone(); }
    @Override public byte[] serializedCatalogBinding() { return serializedCatalogBinding.clone(); }
}
