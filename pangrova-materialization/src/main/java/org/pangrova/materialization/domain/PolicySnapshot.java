package org.pangrova.materialization.domain;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import org.pangrova.spi.materialization.SourceRef;

/**
 * Policy fingerprints observed for every source of a definition.
 *
 * <p>Captured at planning time and re-checked before publication. A mismatch
 * means row filters or column masks changed while the result was being built;
 * the attempt must fail rather than publish data computed under stale policy.
 *
 * @param fingerprints source-to-policy-fingerprint mapping
 */
public record PolicySnapshot(Map<SourceRef, String> fingerprints) {

    public PolicySnapshot {
        if (fingerprints == null) {
            throw new IllegalArgumentException("fingerprints must not be null");
        }
        TreeMap<SourceRef, String> copy = new TreeMap<>(Comparator.comparing(SourceRef::canonicalName));
        for (Map.Entry<SourceRef, String> entry : fingerprints.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null || entry.getValue().isBlank()) {
                throw new IllegalArgumentException("policy snapshot entries must be complete");
            }
            copy.put(entry.getKey(), entry.getValue());
        }
        fingerprints = Map.copyOf(copy);
    }

    /**
     * Returns the canonical SHA-256 fingerprint of this policy snapshot.
     *
     * @return lowercase hex fingerprint
     */
    public String fingerprint() {
        StringBuilder canonical = new StringBuilder("policies:");
        fingerprints.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(java.util.Comparator.comparing(SourceRef::canonicalName)))
                .forEach(entry -> canonical.append('|')
                        .append(entry.getKey().canonicalName())
                        .append('=')
                        .append(entry.getValue()));
        return CanonicalHash.sha256(canonical.toString());
    }
}
