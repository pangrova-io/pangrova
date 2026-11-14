package org.pangrova.shuffle.security;

import org.pangrova.common.util.DigestUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.security.enums.CapabilityOperation;

/** Issues and verifies service-signed, owner-fenced shuffle capabilities. */
public final class ShuffleCapabilityAuthority {

    private static final int MAGIC = 0x50475343;
    private static final int VERSION = 2;
    private static final int SIGNATURE_BYTES = 32;
    private static final int MAXIMUM_ATTEMPT_BYTES = 16_384;
    private static final int MINIMUM_SECRET_BYTES = 32;
    private static final int MAXIMUM_SECRET_BYTES = 4096;

    private final byte[] secret;

    private ShuffleCapabilityAuthority(byte[] secret) {
        if (secret.length < MINIMUM_SECRET_BYTES || secret.length > MAXIMUM_SECRET_BYTES) {
            throw new IllegalArgumentException(
                    "capability secret must contain between 32 and 4096 bytes");
        }
        this.secret = secret.clone();
    }

    /** Loads HMAC key material from a mounted secret file. */
    public static ShuffleCapabilityAuthority load(Path path) throws IOException {
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException("capability secret must be a readable file");
        }
        return new ShuffleCapabilityAuthority(Files.readAllBytes(path));
    }

    /** Creates an authority from explicit key bytes for isolated tests. */
    public static ShuffleCapabilityAuthority testing(byte[] secret) {
        return new ShuffleCapabilityAuthority(secret);
    }

    /** Issues an opaque capability bound to the exact operation and owner fence. */
    public ShuffleAccessCapability issue(CapabilityScope scope, long expiresAtEpochMs) {
        if (expiresAtEpochMs <= System.currentTimeMillis()) {
            throw new IllegalArgumentException("capability expiry must be in the future");
        }
        byte[] payload = encode(scope, expiresAtEpochMs);
        byte[] signature = sign(payload);
        return ShuffleAccessCapability.newBuilder()
                .setToken(ByteString.copyFrom(payload).concat(ByteString.copyFrom(signature)))
                .setExpiresAtEpochMs(expiresAtEpochMs)
                .build();
    }

    /** Verifies signature, expiry, and exact expected scope. */
    public void verify(
            ShuffleAccessCapability capability,
            CapabilityScope expected,
            long nowEpochMs) {
        if (capability == null || capability.getToken().size() <= SIGNATURE_BYTES
                || capability.getExpiresAtEpochMs() <= nowEpochMs) {
            throw new SecurityException("shuffle capability is missing or expired");
        }
        byte[] token = capability.getToken().toByteArray();
        int payloadLength = token.length - SIGNATURE_BYTES;
        byte[] payload = java.util.Arrays.copyOf(token, payloadLength);
        byte[] signature = java.util.Arrays.copyOfRange(token, payloadLength, token.length);
        if (!MessageDigest.isEqual(signature, sign(payload))) {
            throw new SecurityException("shuffle capability signature is invalid");
        }
        DecodedCapability decoded = decode(payload);
        if (!decoded.scope().equals(expected)
                || decoded.expiresAtEpochMs() != capability.getExpiresAtEpochMs()) {
            throw new SecurityException("shuffle capability scope is invalid");
        }
    }

    private static byte[] encode(CapabilityScope scope, long expiresAtEpochMs) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeByte(VERSION);
                output.writeByte(scope.operation().ordinal());
                output.writeUTF(scope.subject());
                output.writeUTF(scope.ownerId());
                output.writeUTF(scope.ownerIncarnation());
                output.writeLong(scope.ownerFencingToken());
                output.writeLong(expiresAtEpochMs);
                if (scope.taskAttempt() == null) {
                    output.writeInt(-1);
                } else {
                    byte[] attempt = scope.taskAttempt().toByteArray();
                    if (attempt.length > MAXIMUM_ATTEMPT_BYTES) {
                        throw new IllegalArgumentException("task attempt exceeds capability limit");
                    }
                    output.writeInt(attempt.length);
                    output.write(attempt);
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("failed to encode shuffle capability", impossible);
        }
    }

    private static DecodedCapability decode(byte[] payload) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (input.readInt() != MAGIC || input.readUnsignedByte() != VERSION) {
                throw new SecurityException("shuffle capability version is invalid");
            }
            int operation = input.readUnsignedByte();
            if (operation >= CapabilityOperation.values().length) {
                throw new SecurityException("shuffle capability operation is invalid");
            }
            String subject = input.readUTF();
            String ownerId = input.readUTF();
            String ownerIncarnation = input.readUTF();
            long ownerFencingToken = input.readLong();
            long expiry = input.readLong();
            int attemptLength = input.readInt();
            if (attemptLength < -1 || attemptLength > MAXIMUM_ATTEMPT_BYTES) {
                throw new SecurityException("shuffle capability task attempt length is invalid");
            }
            TaskAttemptId attempt = attemptLength == -1 ? null
                    : TaskAttemptId.parseFrom(input.readNBytes(attemptLength));
            CapabilityScope scope = new CapabilityScope(
                    CapabilityOperation.values()[operation],
                    subject, ownerId, ownerIncarnation, ownerFencingToken, attempt);
            if (input.available() != 0) {
                throw new SecurityException("shuffle capability has trailing data");
            }
            return new DecodedCapability(scope, expiry);
        } catch (IOException | IllegalArgumentException failure) {
            throw new SecurityException("shuffle capability encoding is invalid", failure);
        }
    }

    private byte[] sign(byte[] payload) {
        return DigestUtils.hmacSha256(secret).doFinal(payload);
    }

    private record DecodedCapability(CapabilityScope scope, long expiresAtEpochMs) {}
}
