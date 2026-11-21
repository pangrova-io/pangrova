package org.pangrova.protocol.catalog;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.Checksum;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Comparator;

/** Canonical SHA-256 fingerprint for an unresolved catalog deployment. */
public final class CatalogDeploymentFingerprint {

    private CatalogDeploymentFingerprint() {}

    /** Computes the canonical deployment fingerprint before local secret resolution. */
    public static Checksum checksum(CatalogDeploymentResponse descriptor) {
        StringBuilder canonical = new StringBuilder()
                .append(descriptor.catalogName()).append('\n')
                .append(descriptor.connectorProvider()).append('\n')
                .append(descriptor.catalogIncarnationId()).append('\n')
                .append(descriptor.catalogRevision()).append('\n');
        descriptor.properties().entrySet().stream()
                .sorted(java.util.Map.Entry.comparingByKey())
                .forEach(entry -> appendPart(canonical, entry.getKey(), entry.getValue()));
        descriptor.allowedTenants().stream().sorted(Comparator.naturalOrder())
                .forEach(tenant -> appendPart(canonical, "tenant", tenant));
        return ProtocolChecksums.sha256(canonical.toString().getBytes(StandardCharsets.UTF_8));
    }

    /** Verifies an expected SHA-256 fingerprint against the catalog deployment. */
    public static boolean matches(Checksum expected, CatalogDeploymentResponse descriptor) {
        Checksum actual = checksum(descriptor);
        return expected.getAlgorithm() == Checksum.Algorithm.ALGORITHM_SHA256
                && expected.getDigest().size() == 32
                && MessageDigest.isEqual(
                        expected.getDigest().toByteArray(), actual.getDigest().toByteArray());
    }

    private static void appendPart(StringBuilder target, String key, String value) {
        target.append(key.length()).append(':').append(key)
                .append(value.length()).append(':').append(value).append('\n');
    }
}
