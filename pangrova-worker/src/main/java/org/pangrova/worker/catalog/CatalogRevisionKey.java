package org.pangrova.worker.catalog;

import org.pangrova.protocol.v2.CatalogExecutionBinding;

/** Exact, immutable tenant-scoped identity of a deployed catalog revision. */
record CatalogRevisionKey(String tenantId, String name, String provider,
                          String incarnation, long revision, byte[] fingerprint) {
    CatalogRevisionKey {
        fingerprint = fingerprint.clone();
    }

    static CatalogRevisionKey from(CatalogExecutionBinding binding, String tenantId) {
        var fingerprint = binding.getConfigurationFingerprint();
        if (fingerprint.getAlgorithm() != org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                || fingerprint.getDigest().size() != 32) {
            throw new IllegalArgumentException("Invalid catalog deployment fingerprint");
        }
        return new CatalogRevisionKey(tenantId, binding.getCatalogName(), binding.getConnectorProvider(),
                binding.getCatalogIncarnationId(), binding.getCatalogRevision(),
                fingerprint.getDigest().toByteArray());
    }

    @Override
    public byte[] fingerprint() { return fingerprint.clone(); }

    @Override
    public boolean equals(Object other) {
        return other instanceof CatalogRevisionKey that
                && revision == that.revision
                && tenantId.equals(that.tenantId)
                && name.equals(that.name)
                && provider.equals(that.provider)
                && incarnation.equals(that.incarnation)
                && java.util.Arrays.equals(fingerprint, that.fingerprint);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(tenantId, name, provider, incarnation, revision,
                java.util.Arrays.hashCode(fingerprint));
    }
}
