package org.pangrova.protocol;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.AcceptedSourceSnapshotRef;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.spi.connector.metadata.AcceptedSourceSnapshot;
import org.pangrova.spi.connector.metadata.ManagedSourceCatalog;

/** Canonical source membership encoding shared by write allocation, metadata RPC and physical split readers. */
public final class SourceSnapshotBindings {
    private SourceSnapshotBindings() {}

    /** Encodes immutable accepted membership under the exact current authorized catalog binding. */
    public static AcceptedSourceSnapshotRef encode(AcceptedSourceSnapshot reference, CatalogExecutionBinding catalog) {
        requireCatalog(catalog);
        if (!reference.deployment().catalog().equals(catalog.getCatalogName())
                || !reference.deployment().incarnation().equals(catalog.getCatalogIncarnationId())
                || reference.deployment().revision() != catalog.getCatalogRevision()) {
            throw new IllegalArgumentException("Accepted source belongs to another catalog deployment");
        }
        return AcceptedSourceSnapshotRef.newBuilder().setCatalog(catalog).setTableId(reference.tableId())
                .setTableGeneration(reference.generation()).setDatasetUri(reference.datasetUri())
                .setVersion(reference.version())
                .setManifestRelativePath(reference.manifestRelativePath())
                .setManifestSha256(checksum(reference.manifestSha256()))
                .setPhysicalSchemaSha256(checksum(reference.schemaSha256())).setSizeBytes(reference.manifestSize())
                .setNamingScheme(reference.namingScheme()).setMembershipId(reference.membershipId())
                .setTenantId(reference.tenant())
                .setAcceptedOriginCatalogRevision(reference.acceptedOriginRevision()).build();
    }

    /** Decodes the canonical membership proof and rejects another deployment or unknown fields. */
    public static AcceptedSourceSnapshot decode(AcceptedSourceSnapshotRef reference, CatalogExecutionBinding catalog) {
        requireCatalog(catalog);
        if (!reference.getUnknownFields().asMap().isEmpty() || !reference.hasCatalog()
                || !reference.getCatalog().equals(catalog)) {
            throw new IllegalArgumentException("Accepted source catalog binding differs");
        }
        return new AcceptedSourceSnapshot(new ManagedSourceCatalog.Deployment(catalog.getCatalogName(),
                catalog.getCatalogIncarnationId(), catalog.getCatalogRevision()), reference.getTenantId(),
                reference.getTableId(),
                reference.getTableGeneration(), reference.getDatasetUri(), reference.getVersion(),
                reference.getManifestRelativePath(),
                digest(reference.getManifestSha256()), digest(reference.getPhysicalSchemaSha256()),
                reference.getSizeBytes(),
                reference.getNamingScheme(), reference.getMembershipId(), reference.getAcceptedOriginCatalogRevision());
    }

    private static void requireCatalog(CatalogExecutionBinding catalog) {
        if (!catalog.getUnknownFields().asMap().isEmpty() || catalog.getCatalogName().isBlank()
                || !"lance".equals(catalog.getConnectorProvider())
                || catalog.getCatalogIncarnationId().isBlank() || catalog.getCatalogRevision() <= 0) {
            throw new IllegalArgumentException("Accepted source needs a complete Lance execution deployment");
        }
        digest(catalog.getConfigurationFingerprint());
    }

    private static Checksum checksum(byte[] bytes) {
        return Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(bytes))
                .build();
    }
    private static byte[] digest(Checksum checksum) {
        if (!checksum.getUnknownFields().asMap().isEmpty()
                || checksum.getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != 32) {
            throw new IllegalArgumentException("Accepted source requires SHA-256 evidence");
        }
        return checksum.getDigest().toByteArray();
    }
}
