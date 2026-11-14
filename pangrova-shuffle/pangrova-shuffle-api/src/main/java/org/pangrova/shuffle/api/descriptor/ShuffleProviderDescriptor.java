package org.pangrova.shuffle.api.descriptor;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Base64;
import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable, credential-free allocation evidence. Only the issuing provider interprets its canonical bytes. */
@Experimental(reason = "Durable external exchange allocation evidence")
public record ShuffleProviderDescriptor(String providerId, int version, String payloadBase64) {
    /** Upper bound for one provider allocation across every journal and replay path. */
    public static final int MAX_BYTES = 1024 * 1024;
    private static final Pattern PROVIDER_ID = Pattern.compile("[a-z][a-z0-9-]{0,63}");

    public ShuffleProviderDescriptor {
        Objects.requireNonNull(providerId, "providerId");
        Objects.requireNonNull(payloadBase64, "payloadBase64");
        if (!PROVIDER_ID.matcher(providerId).matches() || version < 1 || payloadBase64.isEmpty()
                || payloadBase64.length() > 4 * ((MAX_BYTES + 2) / 3)) {
            throw new IllegalArgumentException("Invalid shuffle provider descriptor identity, version or size");
        }
        byte[] decoded = Base64.getDecoder().decode(payloadBase64);
        if (decoded.length > MAX_BYTES || !Base64.getEncoder().encodeToString(decoded).equals(payloadBase64)) {
            throw new IllegalArgumentException("Invalid shuffle provider descriptor encoding or size");
        }
    }

    /** Copies the provider's serialized allocation into an immutable envelope. */
    public static ShuffleProviderDescriptor of(String providerId, int version, byte[] payload) {
        if (payload.length > MAX_BYTES) throw new IllegalArgumentException("Shuffle descriptor exceeds size limit");
        return new ShuffleProviderDescriptor(providerId, version, Base64.getEncoder().encodeToString(payload));
    }

    /** Returns an independent copy for the issuing provider to verify and decode. */
    public byte[] payload() {
        return Base64.getDecoder().decode(payloadBase64);
    }
}
