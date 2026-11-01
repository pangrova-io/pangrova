package org.pangrova.streaming.property.cdc;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.spi.streaming.record.CdcEnvelope;
import org.pangrova.spi.streaming.record.CdcOperation;
import org.pangrova.spi.streaming.record.CdcRecordConversionException;
import org.pangrova.spi.streaming.record.CdcRecordConverter;
import org.pangrova.spi.streaming.record.CdcRow;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 23: Dead-letter routing in convertBatch.
 *
 * <p>Validates: Requirements 10.2, 16.1</p>
 *
 * <p>For any batch of N payloads where K are valid and (N-K) are malformed,
 * convertBatch() returns exactly K successfully parsed CdcEnvelopes and
 * invokes the dead-letter callback exactly (N-K) times.</p>
 */
class DeadLetterRoutingPropertyTest {

    private static final byte[] VALID_MARKER = new byte[]{1};
    private static final byte[] INVALID_MARKER = new byte[]{0};

    /**
     * Validates: Requirements 10.2, 16.1
     *
     * Given N payloads with K valid, convertBatch returns K envelopes
     * and fires (N-K) dead-letter callbacks.
     */
    @Property(tries = 10)
    void deadLetterCountMatchesInvalidPayloads(
            @ForAll @IntRange(min = 0, max = 50) int validCount,
            @ForAll @IntRange(min = 0, max = 50) int invalidCount) {
        int totalCount = validCount + invalidCount;
        Assume.that(totalCount > 0);

        CdcRecordConverter converter = validInvalidStubConverter();
        List<byte[]> payloads = buildPayloads(validCount, invalidCount);

        AtomicInteger deadLetterCount = new AtomicInteger(0);
        List<byte[]> deadLetteredPayloads = new ArrayList<>();

        List<CdcEnvelope> envelopes = converter.convertBatch(
                payloads,
                (payload, ex) -> {
                    deadLetterCount.incrementAndGet();
                    deadLetteredPayloads.add(payload);
                });

        assertThat(envelopes).hasSize(validCount);
        assertThat(deadLetterCount.get()).isEqualTo(invalidCount);
    }

    /**
     * Validates: Requirements 10.2
     *
     * All returned envelopes are non-null and well-formed.
     */
    @Property(tries = 10)
    void allReturnedEnvelopesAreValid(
            @ForAll @IntRange(min = 1, max = 30) int validCount,
            @ForAll @IntRange(min = 0, max = 30) int invalidCount) {
        CdcRecordConverter converter = validInvalidStubConverter();
        List<byte[]> payloads = buildPayloads(validCount, invalidCount);

        List<CdcEnvelope> envelopes = converter.convertBatch(
                payloads,
                (payload, ex) -> { /* discard */ });

        for (CdcEnvelope envelope : envelopes) {
            assertThat(envelope).isNotNull();
            assertThat(envelope.operation()).isNotNull();
            assertThat(envelope.tableName()).isNotNull();
        }
    }

    /**
     * Validates: Requirements 16.1
     *
     * Each dead-letter callback receives the original failing payload
     * and a non-null exception.
     */
    @Property(tries = 10)
    void deadLetterCallbackReceivesPayloadAndException(
            @ForAll @IntRange(min = 0, max = 20) int validCount,
            @ForAll @IntRange(min = 1, max = 20) int invalidCount) {
        CdcRecordConverter converter = validInvalidStubConverter();
        List<byte[]> payloads = buildPayloads(validCount, invalidCount);

        List<byte[]> capturedPayloads = new ArrayList<>();
        List<Exception> capturedExceptions = new ArrayList<>();

        converter.convertBatch(
                payloads,
                (payload, ex) -> {
                    capturedPayloads.add(payload);
                    capturedExceptions.add(ex);
                });

        assertThat(capturedPayloads).hasSize(invalidCount);
        assertThat(capturedExceptions).hasSize(invalidCount);
        for (int i = 0; i < invalidCount; i++) {
            assertThat(capturedPayloads.get(i)).isEqualTo(INVALID_MARKER);
            assertThat(capturedExceptions.get(i)).isNotNull();
        }
    }

    /**
     * Validates: Requirements 10.2
     *
     * When all payloads are valid, zero dead-letter callbacks fire.
     */
    @Property(tries = 10)
    void allValidPayloadsProduceZeroDeadLetters(
            @ForAll @IntRange(min = 1, max = 50) int count) {
        CdcRecordConverter converter = validInvalidStubConverter();
        List<byte[]> payloads = buildPayloads(count, 0);

        AtomicInteger deadLetterCount = new AtomicInteger(0);
        List<CdcEnvelope> envelopes = converter.convertBatch(
                payloads,
                (payload, ex) -> deadLetterCount.incrementAndGet());

        assertThat(envelopes).hasSize(count);
        assertThat(deadLetterCount.get()).isZero();
    }

    /**
     * Validates: Requirements 10.2
     *
     * When all payloads are invalid, zero envelopes returned and N callbacks fire.
     */
    @Property(tries = 10)
    void allInvalidPayloadsProduceZeroEnvelopes(
            @ForAll @IntRange(min = 1, max = 50) int count) {
        CdcRecordConverter converter = validInvalidStubConverter();
        List<byte[]> payloads = buildPayloads(0, count);

        AtomicInteger deadLetterCount = new AtomicInteger(0);
        List<CdcEnvelope> envelopes = converter.convertBatch(
                payloads,
                (payload, ex) -> deadLetterCount.incrementAndGet());

        assertThat(envelopes).isEmpty();
        assertThat(deadLetterCount.get()).isEqualTo(count);
    }

    /**
     * Build a payload list with valid markers first, then invalid markers.
     * The ordering is deterministic: valid payloads come first.
     */
    private static List<byte[]> buildPayloads(int validCount, int invalidCount) {
        List<byte[]> payloads = new ArrayList<>(validCount + invalidCount);
        for (int i = 0; i < validCount; i++) {
            payloads.add(VALID_MARKER);
        }
        for (int i = 0; i < invalidCount; i++) {
            payloads.add(INVALID_MARKER);
        }
        return payloads;
    }

    /**
     * Stub converter that succeeds for VALID_MARKER payloads and throws
     * CdcRecordConversionException for INVALID_MARKER payloads.
     * No mock frameworks — real stub implementation per project standards.
     */
    private static CdcRecordConverter validInvalidStubConverter() {
        return new CdcRecordConverter() {
            @Override
            public String formatId() {
                return "test-stub";
            }

            @Override
            public CdcEnvelope deserialize(byte[] payload) {
                if (payload.length > 0 && payload[0] == 1) {
                    return new CdcEnvelope(
                            CdcOperation.INSERT,
                            "test-table",
                            null,
                            CdcRow.of(Map.of("id", 1)),
                            Instant.EPOCH,
                            0,
                            Map.of());
                }
                throw new CdcRecordConversionException(
                        "Invalid payload: marker byte is not 1", null);
            }
        };
    }
}
