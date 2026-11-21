package org.pangrova.file.api.access;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/** One authorized file operation, ready to send to the named Worker. */
@Experimental
public record FileAccessGrant(String workerUrl, String descriptor, String signature,
        String operation, String path, long expiresAtEpochMillis) {

    @Override
    public String toString() {
        return "FileAccessGrant[operation=" + operation + ", expiresAtEpochMillis=" + expiresAtEpochMillis + "]";
    }

    public Instant expiresAt() {
        return Instant.ofEpochMilli(expiresAtEpochMillis);
    }
}
