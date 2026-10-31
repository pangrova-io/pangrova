package org.pangrova.file.api.upload;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/**
 * One provider URL and its provider-reported conservative expiry bound.
 *
 * @param url credential-bearing delivery URL, never suitable for logs
 * @param expiresAtEpochMillis expiry reported by the actual signing operation, not inferred from requested TTL
 */
@Experimental
public record PresignedObjectUrl(String url, long expiresAtEpochMillis) {
    public PresignedObjectUrl {
        Objects.requireNonNull(url, "url");
        if (url.isBlank() || expiresAtEpochMillis <= 0) {
            throw new IllegalArgumentException("Provider URL requires a nonblank URL and positive expiry");
        }
    }

    /** Omits the URL and its bearer credentials. */
    @Override public String toString() {
        return "PresignedObjectUrl[expiresAtEpochMillis=" + expiresAtEpochMillis + "]";
    }
}
