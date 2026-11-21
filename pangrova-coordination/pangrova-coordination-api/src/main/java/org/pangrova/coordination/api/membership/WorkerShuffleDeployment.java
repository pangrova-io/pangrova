package org.pangrova.coordination.api.membership;

import org.pangrova.common.api.annotation.Experimental;

/** Shuffle configuration actually installed for this exact registered Worker process. */
@Experimental
public record WorkerShuffleDeployment(String mode, String provider) {
    public WorkerShuffleDeployment {
        if ("NATIVE".equals(mode) || "LOCAL".equals(mode)) {
            if (provider != null) throw new IllegalArgumentException("Worker-owned shuffle has no external provider");
        } else if (!"REMOTE".equals(mode) || provider == null || !provider.matches("[a-z][a-z0-9-]*")) {
            throw new IllegalArgumentException("Remote shuffle requires its installed provider ID");
        }
    }
}
