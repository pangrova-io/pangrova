package org.pangrova.protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.v2.StageResourceGrant;

import java.util.Base64;
import java.util.Objects;

/** JSON-boundary codec for the sole canonical task resource grant message. */
public final class StageResourceGrantCodec {

    private StageResourceGrantCodec() {}

    public static String encode(StageResourceGrant grant) {
        validate(Objects.requireNonNull(grant, "grant must not be null"));
        return Base64.getEncoder().encodeToString(grant.toByteArray());
    }

    /**
     * Decodes and validates a required canonical resource grant.
     *
     * @param encoded Base64-encoded grant
     * @return decoded grant
     * @throws IllegalArgumentException if the value is absent or malformed
     */
    public static StageResourceGrant decode(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalArgumentException("canonical resource grant is required");
        }
        try {
            StageResourceGrant grant = StageResourceGrant.parseFrom(
                    Base64.getDecoder().decode(encoded));
            validate(grant);
            return grant;
        } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "resource grant is not a canonical StageResourceGrant", failure);
        }
    }

    public static StageResourceGrant decodeOptional(String encoded) {
        return encoded == null || encoded.isBlank()
                ? StageResourceGrant.getDefaultInstance()
                : decode(encoded);
    }

    /**
     * Validates the required identity and positive resource quantities.
     *
     * @param grant grant to validate
     * @throws IllegalArgumentException if required fields are absent
     */
    public static void validate(StageResourceGrant grant) {
        Objects.requireNonNull(grant, "grant must not be null");
        if (grant.getGrantId().isBlank()) {
            throw new IllegalArgumentException("resource grant identity is required");
        }
        if (grant.getCpuSlots() == 0 || grant.getNativeMemoryBytes() == 0) {
            throw new IllegalArgumentException(
                    "resource grant requires positive cpu_slots and native_memory_bytes");
        }
    }
}
