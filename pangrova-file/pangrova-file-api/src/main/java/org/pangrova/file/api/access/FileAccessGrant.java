package org.pangrova.file.api.access;

import java.time.Instant;

/** One authorized file operation, ready to send to the named Worker. */
public record FileAccessGrant(String workerUrl, String descriptor, String signature,
        String operation, String path, long expiresAtEpochMillis) {

    public Instant expiresAt() {
        return Instant.ofEpochMilli(expiresAtEpochMillis);
    }
}
