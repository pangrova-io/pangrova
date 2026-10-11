package org.pangrova.coordinator.metadata;

import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DatasetScanPartitionsResult;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.spi.connector.metadata.NativeDatasetScanPage;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.OptionalLong;

/** Validates bounded Worker manifest descriptors before exposing them to a connector. */
final class NativeDatasetScanPages {
    private NativeDatasetScanPages() {}

    static NativeDatasetScanPage decode(RuntimeCommandResponse response, CatalogExecutionBinding catalog,
            org.pangrova.spi.connector.metadata.AcceptedSourceSnapshot snapshot, OptionalLong after, int pageSize) {
        long version = snapshot.version(); byte[] schema = snapshot.schemaSha256();
        var expected = org.pangrova.protocol.SourceSnapshotBindings.encode(snapshot, catalog);
        try {
            var result = response.getPayload().unpack(DatasetScanPartitionsResult.class);
            requireSha256(result.getManifestSha256());
            requireSha256(result.getPhysicalSchemaSha256());
            if (!result.hasAcceptedSnapshot() || !result.getAcceptedSnapshot().equals(expected)
                    || !MessageDigest.isEqual(snapshot.manifestSha256(), result.getManifestSha256().getDigest().toByteArray())
                    || result.getVersion() != version || result.getTotalPartitions() < 0
                    || result.getPartitionsCount() > pageSize
                    || !MessageDigest.isEqual(schema, result.getPhysicalSchemaSha256().getDigest().toByteArray())) {
                throw new IllegalArgumentException("Native scan page differs from its pinned request");
            }
            if (result.getPartitionsList().stream().anyMatch(partition -> !partition.getAcceptedSnapshot().equals(expected))) {
                throw new IllegalArgumentException("Partition omitted or changed its accepted membership");
            }
            var partitions = descriptors(result, after);
            OptionalLong next = result.getHasMore()
                    ? OptionalLong.of(result.getNextPartitionId()) : OptionalLong.empty();
            return new NativeDatasetScanPage(version, result.getManifestSha256().getDigest().toByteArray(), schema,
                    catalog.toByteArray(), result.getTotalPartitions(), partitions, next);
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Native scan page response is malformed", malformed);
        }
    }

    private static ArrayList<NativeDatasetScanPage.Partition> descriptors(
            DatasetScanPartitionsResult page, OptionalLong after) {
        var result = new ArrayList<NativeDatasetScanPage.Partition>();
        long previous = after.orElse(-1);
        for (var partition : page.getPartitionsList()) {
            if (partition.getPartitionId() <= previous || partition.getPhysicalRows() < 0
                    || !page.getManifestSha256().equals(partition.getManifestSha256())
                    || !page.getPhysicalSchemaSha256().equals(partition.getPhysicalSchemaSha256())) {
                throw new IllegalArgumentException("Native scan partitions are duplicated or inconsistent");
            }
            result.add(new NativeDatasetScanPage.Partition(partition.getPartitionId(), partition.getPhysicalRows()));
            previous = partition.getPartitionId();
        }
        return result;
    }

    private static void requireSha256(Checksum value) {
        if (value.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256 || value.getDigest().size() != 32) {
            throw new IllegalArgumentException("Native scan page requires exact SHA-256 identities");
        }
    }
}
