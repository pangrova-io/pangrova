package org.pangrova.transport.flight;

import org.pangrova.transport.flight.exception.TicketValidationException;

import org.pangrova.common.crypto.Sha256Digests;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.protocol.transport.DataTicket;

/**
 * Signs and validates transport tickets using HMAC-SHA256.
 *
 * <p>Signed tickets include the original ticket payload, an expiry timestamp,
 * and a cryptographic signature. The shared secret is rotatable via configuration
 * reload without server restart.</p>
 *
 * <p>Wire format of a signed ticket:
 * <pre>
 *   [4 bytes: payload length]
 *   [N bytes: original DataTicket serialized payload]
 *   [8 bytes: expiry epoch millis]
 *   [32 bytes: HMAC-SHA256 signature over (payload + expiry)]
 * </pre>
 *
 * <p>Concurrency: thread-safe. The secret key is volatile for rotation support.
 * Each signing/validation call creates a fresh Mac instance (Mac is not thread-safe).</p>
 */
@ThreadSafe
public final class TransportTicketSigner {

    private static final int SIGNATURE_LENGTH = 32;

    private final Duration ticketTtl;
    private final Clock clock;
    private volatile byte[] secretKey;

    /**
     * Creates a ticket signer with the given shared secret and TTL.
     *
     * @param sharedSecret the HMAC shared secret (must be at least 32 bytes for security)
     * @param ticketTtl    how long a signed ticket remains valid
     * @param clock        clock for timestamp generation (injectable for testing)
     */
    public TransportTicketSigner(byte[] sharedSecret, Duration ticketTtl, Clock clock) {
        Objects.requireNonNull(sharedSecret, "sharedSecret must not be null");
        if (sharedSecret.length < 32) {
            throw new IllegalArgumentException(
                    "sharedSecret must be at least 32 bytes, got: " + sharedSecret.length);
        }
        Objects.requireNonNull(ticketTtl, "ticketTtl must not be null");
        if (ticketTtl.isNegative() || ticketTtl.isZero()) {
            throw new IllegalArgumentException("ticketTtl must be positive");
        }
        Objects.requireNonNull(clock, "clock must not be null");
        this.secretKey = Arrays.copyOf(sharedSecret, sharedSecret.length);
        this.ticketTtl = ticketTtl;
        this.clock = clock;
    }

    /**
     * Creates a ticket signer with the system clock.
     */
    public TransportTicketSigner(byte[] sharedSecret, Duration ticketTtl) {
        this(sharedSecret, ticketTtl, Clock.systemUTC());
    }

    /**
     * Creates a ticket signer from a string secret (UTF-8 encoded, padded to 32 bytes minimum).
     */
    public TransportTicketSigner(String sharedSecret, Duration ticketTtl) {
        this(deriveKey(sharedSecret), ticketTtl, Clock.systemUTC());
    }

    /**
     * Rotates the signing secret. Existing tickets signed with the old secret
     * will fail validation after rotation.
     */
    public void rotateSecret(byte[] newSecret) {
        Objects.requireNonNull(newSecret, "newSecret must not be null");
        if (newSecret.length < 32) {
            throw new IllegalArgumentException(
                    "newSecret must be at least 32 bytes, got: " + newSecret.length);
        }
        this.secretKey = Arrays.copyOf(newSecret, newSecret.length);
    }

    /**
     * Signs a DataTicket, producing a byte array containing the payload,
     * expiry timestamp, and HMAC-SHA256 signature.
     */
    public byte[] sign(DataTicket ticket) {
        Objects.requireNonNull(ticket, "ticket must not be null");
        byte[] payload = ticket.serialize();
        long expiryMillis = clock.instant().plus(ticketTtl).toEpochMilli();

        try (var baos = new ByteArrayOutputStream();
             var out = new DataOutputStream(baos)) {
            out.writeInt(payload.length);
            out.write(payload);
            out.writeLong(expiryMillis);
            out.flush();

            byte[] dataToSign = baos.toByteArray();
            byte[] signature = Sha256Digests.hmacSha256(secretKey).doFinal(dataToSign);

            baos.write(signature);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to sign ticket", e);
        }
    }

    /**
     * Validates and deserializes a signed ticket.
     *
     * @param signedTicket the signed ticket bytes
     * @return the deserialized DataTicket if valid
     * @throws TicketValidationException if the ticket is unsigned, tampered, or expired
     */
    public DataTicket validate(byte[] signedTicket) {
        Objects.requireNonNull(signedTicket, "signedTicket must not be null");

        if (signedTicket.length < 4 + 8 + SIGNATURE_LENGTH) {
            throw new TicketValidationException("Ticket too short to contain valid signature");
        }

        try (var in = new DataInputStream(new ByteArrayInputStream(signedTicket))) {
            int payloadLength = in.readInt();
            if (payloadLength <= 0 || payloadLength > signedTicket.length - 4 - 8 - SIGNATURE_LENGTH) {
                throw new TicketValidationException("Invalid payload length in signed ticket");
            }

            byte[] payload = new byte[payloadLength];
            in.readFully(payload);
            long expiryMillis = in.readLong();
            byte[] receivedSignature = new byte[SIGNATURE_LENGTH];
            in.readFully(receivedSignature);

            // Reconstruct the data that was signed
            byte[] dataToVerify = Arrays.copyOf(signedTicket,
                    signedTicket.length - SIGNATURE_LENGTH);
            byte[] expectedSignature = Sha256Digests.hmacSha256(secretKey).doFinal(dataToVerify);

            // Constant-time comparison to prevent timing attacks
            if (!MessageDigest.isEqual(expectedSignature, receivedSignature)) {
                throw new TicketValidationException("Invalid ticket signature");
            }

            // Check expiry
            Instant expiry = Instant.ofEpochMilli(expiryMillis);
            if (clock.instant().isAfter(expiry)) {
                throw new TicketValidationException(
                        "Ticket expired at " + expiry + ", current time: " + clock.instant());
            }

            return DataTicket.deserialize(payload);
        } catch (IOException e) {
            throw new TicketValidationException("Failed to parse signed ticket", e);
        }
    }



    private static byte[] deriveKey(String secret) {
        Objects.requireNonNull(secret, "secret must not be null");
        if (secret.isBlank()) {
            throw new IllegalArgumentException("secret must not be blank");
        }
        byte[] raw = secret.getBytes(StandardCharsets.UTF_8);
        if (raw.length >= 32) {
            return raw;
        }
        // F-SEC-06: short secrets were previously padded by REPEATING their bytes
        // (padded[i] = raw[i % raw.length]). That adds no entropy, produces a predictable
        // repeating-byte key, and makes distinct secrets collide (e.g. "ab" and "abab"
        // derived the identical key). Derive a well-diffused 32-byte key via SHA-256 instead.
        // This does not manufacture entropy — a short secret is still weak, so production
        // callers should supply >= 32 random bytes (enforced by the byte[] constructors).
        return Sha256Digests.sha256().digest(raw);
    }
}
