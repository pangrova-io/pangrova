package org.pangrova.shuffle.api;

import java.util.Arrays;

/** Immutable value object for opaque external-shuffle authorization. */
public final class ShuffleAccess {

    private final byte[] token;
    private final long expiresAtEpochMs;

    /** Creates opaque authorization that remains valid until the supplied epoch millisecond. */
    public ShuffleAccess(byte[] token, long expiresAtEpochMs) {
        if (token == null || token.length == 0) {
            throw new IllegalArgumentException("shuffle access token must not be empty");
        }
        if (expiresAtEpochMs <= 0) {
            throw new IllegalArgumentException("shuffle access expiry must be positive");
        }
        this.token = Arrays.copyOf(token, token.length);
        this.expiresAtEpochMs = expiresAtEpochMs;
    }

    public byte[] token() {
        return Arrays.copyOf(token, token.length);
    }

    public long expiresAtEpochMs() {
        return expiresAtEpochMs;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ShuffleAccess access
                && expiresAtEpochMs == access.expiresAtEpochMs
                && Arrays.equals(token, access.token);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(token) + Long.hashCode(expiresAtEpochMs);
    }

    @Override
    public String toString() {
        return "ShuffleAccess[expiresAtEpochMs=" + expiresAtEpochMs + "]";
    }
}
