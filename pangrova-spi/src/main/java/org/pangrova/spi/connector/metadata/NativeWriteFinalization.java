package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;

import java.util.Arrays;
import java.util.Objects;

/** Exact external publication evidence; UNKNOWN does not authorize a catalog commit. */
@Experimental
public record NativeWriteFinalization(Outcome outcome, long publishedVersion, byte[] manifestSha256) {
    /** Mutually exclusive external commit outcomes. */
    @Experimental
    public enum Outcome { COMMITTED, NOT_COMMITTED, UNKNOWN }

    public NativeWriteFinalization {
        Objects.requireNonNull(outcome, "outcome must not be null");
        manifestSha256 = Objects.requireNonNull(manifestSha256, "manifestSha256 must not be null").clone();
        if (outcome == Outcome.COMMITTED) {
            if (publishedVersion <= 0 || manifestSha256.length != 32) {
                throw new IllegalArgumentException("Committed publication requires a positive version and SHA-256");
            }
        } else if (publishedVersion != 0 || manifestSha256.length != 0) {
            throw new IllegalArgumentException("Unconfirmed publication cannot carry a version or digest");
        }
    }

    @Override public byte[] manifestSha256() { return manifestSha256.clone(); }

    /** Compares content, not the address of the immutable byte-array copy. */
    @Override public boolean equals(Object other) {
        return other instanceof NativeWriteFinalization result
                && outcome == result.outcome && publishedVersion == result.publishedVersion
                && Arrays.equals(manifestSha256, result.manifestSha256);
    }

    @Override public int hashCode() {
        return 31 * Objects.hash(outcome, publishedVersion) + Arrays.hashCode(manifestSha256);
    }
}
