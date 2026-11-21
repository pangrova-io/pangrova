package org.pangrova.protocol.capability;

import org.pangrova.protocol.v2.CapabilityRef;
import java.util.Objects;

/** Exact semantic capability identity matching with an optional required implementation build pin. */
public final class CapabilityIdentities {
    private CapabilityIdentities() {}

    /** Both semantic identities must agree; a blank required build accepts any advertised build. */
    public static boolean matches(
            CapabilityRef advertised, CapabilityRef required) {
        Objects.requireNonNull(advertised, "advertised capability must not be null");
        Objects.requireNonNull(required, "required capability must not be null");
        return advertised.getCapabilityId().equals(required.getCapabilityId())
                && advertised.getSemanticVersion().equals(required.getSemanticVersion())
                && advertised.getSignatureSha256().equals(required.getSignatureSha256())
                && (required.getImplementationBuildId().isBlank()
                || advertised.getImplementationBuildId()
                        .equals(required.getImplementationBuildId()));
    }

}
