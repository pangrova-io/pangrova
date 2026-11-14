package org.pangrova.transport.flight;

import org.pangrova.transport.flight.exception.TicketValidationException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.pangrova.protocol.transport.DataTicket;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies ticket signing and validation: unsigned rejected, expired rejected, valid accepted.
 */
class TransportTicketSignerTest {

    private static final byte[] SECRET = "this-is-a-32-byte-secret-key!!!!".getBytes();
    private static final Duration TTL = Duration.ofMinutes(5);

    @Test
    void validTicketAccepted() {
        TransportTicketSigner signer = new TransportTicketSigner(SECRET, TTL);
        DataTicket original = new DataTicket("query-1", "task-1", 0, DataTicket.TicketType.SHUFFLE);

        byte[] signed = signer.sign(original);
        DataTicket validated = signer.validate(signed);

        assertThat(validated.queryId()).isEqualTo("query-1");
        assertThat(validated.taskId()).isEqualTo("task-1");
        assertThat(validated.outputPartition()).isEqualTo(0);
        assertThat(validated.type()).isEqualTo(DataTicket.TicketType.SHUFFLE);
    }

    @Test
    void shortSecretsWithRepeatingPatternDeriveDistinctKeys() {
        // F-SEC-06: "ab" and "abab" previously repeat-padded to the SAME 32-byte key, so their
        // signatures collided. SHA-256 key derivation makes distinct secrets produce distinct keys.
        TransportTicketSigner signerAb = new TransportTicketSigner("ab", TTL);
        TransportTicketSigner signerAbab = new TransportTicketSigner("abab", TTL);
        DataTicket ticket = new DataTicket("query-1", "task-1", 0, DataTicket.TicketType.SHUFFLE);

        byte[] sigAb = signerAb.sign(ticket);
        byte[] sigAbab = signerAbab.sign(ticket);

        assertThat(sigAb).isNotEqualTo(sigAbab);
        // Each secret still round-trips under its own derived key.
        assertThat(signerAb.validate(sigAb).queryId()).isEqualTo("query-1");
        assertThat(signerAbab.validate(sigAbab).queryId()).isEqualTo("query-1");
    }

    @Test
    void unsignedTicketRejected() {
        TransportTicketSigner signer = new TransportTicketSigner(SECRET, TTL);
        DataTicket ticket = new DataTicket("query-1", "task-1", 0, DataTicket.TicketType.SHUFFLE);

        // Raw unsigned ticket bytes (not signed)
        byte[] unsigned = ticket.serialize();

        assertThatThrownBy(() -> signer.validate(unsigned))
                .isInstanceOf(TicketValidationException.class);
    }

    @Test
    void expiredTicketRejected() {
        Instant now = Instant.parse("2024-01-01T12:00:00Z");
        Clock signingClock = Clock.fixed(now, ZoneId.of("UTC"));
        TransportTicketSigner signer = new TransportTicketSigner(SECRET, TTL, signingClock);

        DataTicket ticket = new DataTicket("query-1", "task-1", 0, DataTicket.TicketType.RESULT);
        byte[] signed = signer.sign(ticket);

        // Validate with a clock that is 10 minutes in the future (past the 5-min TTL)
        Instant future = now.plus(Duration.ofMinutes(10));
        Clock validationClock = Clock.fixed(future, ZoneId.of("UTC"));
        TransportTicketSigner validator = new TransportTicketSigner(SECRET, TTL, validationClock);

        assertThatThrownBy(() -> validator.validate(signed))
                .isInstanceOf(TicketValidationException.class)
                .hasMessageContaining("expired");
    }

    @Test
    void tamperedTicketRejected() {
        TransportTicketSigner signer = new TransportTicketSigner(SECRET, TTL);
        DataTicket ticket = new DataTicket("query-1", "task-1", 0, DataTicket.TicketType.SHUFFLE);

        byte[] signed = signer.sign(ticket);
        // Tamper with a byte in the middle of the payload
        signed[10] ^= 0xFF;

        assertThatThrownBy(() -> signer.validate(signed))
                .isInstanceOf(TicketValidationException.class)
                .hasMessageContaining("signature");
    }

    @Test
    void wrongSecretRejected() {
        TransportTicketSigner signer = new TransportTicketSigner(SECRET, TTL);
        DataTicket ticket = new DataTicket("query-1", "task-1", 0, DataTicket.TicketType.SHUFFLE);
        byte[] signed = signer.sign(ticket);

        byte[] differentSecret = "another-32-byte-secret-key-here!".getBytes();
        TransportTicketSigner otherSigner = new TransportTicketSigner(differentSecret, TTL);

        assertThatThrownBy(() -> otherSigner.validate(signed))
                .isInstanceOf(TicketValidationException.class)
                .hasMessageContaining("signature");
    }

    @Test
    void secretRotationInvalidatesOldTickets() {
        TransportTicketSigner signer = new TransportTicketSigner(SECRET, TTL);
        DataTicket ticket = new DataTicket("query-1", "task-1", 0, DataTicket.TicketType.SHUFFLE);
        byte[] signed = signer.sign(ticket);

        // Rotate the secret
        byte[] newSecret = "rotated-32-byte-secret-key-here!".getBytes();
        signer.rotateSecret(newSecret);

        assertThatThrownBy(() -> signer.validate(signed))
                .isInstanceOf(TicketValidationException.class)
                .hasMessageContaining("signature");
    }

    @Test
    void tooShortDataRejected() {
        TransportTicketSigner signer = new TransportTicketSigner(SECRET, TTL);

        assertThatThrownBy(() -> signer.validate(new byte[10]))
                .isInstanceOf(TicketValidationException.class)
                .hasMessageContaining("too short");
    }

    @Test
    void secretMustBeAtLeast32Bytes() {
        byte[] shortSecret = "short".getBytes();

        assertThatThrownBy(() -> new TransportTicketSigner(shortSecret, TTL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32 bytes");
    }

    @Test
    void resultTicketRoundTrip() {
        TransportTicketSigner signer = new TransportTicketSigner(SECRET, TTL);
        DataTicket original = new DataTicket("q-abc", "coordinator", 3, DataTicket.TicketType.RESULT);

        byte[] signed = signer.sign(original);
        DataTicket validated = signer.validate(signed);

        assertThat(validated).isEqualTo(original);
    }
}
