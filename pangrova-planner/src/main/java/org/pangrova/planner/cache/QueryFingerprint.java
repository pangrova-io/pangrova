package org.pangrova.planner.cache;

import org.pangrova.common.util.DigestUtils;

/**
 * Semantic fingerprint of a query including security dimensions.
 */
public record QueryFingerprint(
        String baseFingerprint,
        String contextHash,
        String dataVersion,
        String tenantId,
        String userPrincipal,
        String activeRolesHash,
        String rlsPolicyHash,
        String columnMaskHash
) {

    /**
     * Compute composite SHA-256 hash of all fields — used as cache key.
     */
    public String compositeHash() {
        String combined = String.join("|",
                baseFingerprint,
                contextHash,
                dataVersion,
                tenantId,
                userPrincipal,
                activeRolesHash,
                rlsPolicyHash,
                columnMaskHash);
        return DigestUtils.sha256Hex(combined);
    }

    static String sha256(String input) {
        return DigestUtils.sha256Hex(input);
    }
}
