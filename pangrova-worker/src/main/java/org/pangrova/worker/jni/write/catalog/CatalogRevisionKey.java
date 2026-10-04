package org.pangrova.worker.jni.write.catalog;

import org.pangrova.protocol.v2.CatalogExecutionBinding;

/** Exact, immutable tenant-scoped identity of a deployed catalog revision. */
record CatalogRevisionKey(String tenantId, String name, String provider,
                          String incarnation, long revision, byte[] fingerprint) {
    CatalogRevisionKey {
        fingerprint = fingerprint.clone();
    }

    static CatalogRevisionKey from(CatalogExecutionBinding binding, String tenantId) {
        return new CatalogRevisionKey(tenantId, binding.getCatalogName(), binding.getConnectorProvider(),
                binding.getCatalogIncarnationId(), binding.getCatalogRevision(),
                binding.getConfigurationFingerprint().getDigest().toByteArray());
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
