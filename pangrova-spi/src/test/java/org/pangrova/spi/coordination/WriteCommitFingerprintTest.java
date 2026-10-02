package org.pangrova.spi.coordination;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteCommitFingerprintTest {

    private static final byte[] WRITE_BINDING = {42};

    @Test
    void fragmentArrivalOrderDoesNotChangeCommitIdentity() {
        byte[] first = {1, 2, 3};
        byte[] second = {4, 5, 6};

        String left = fingerprint(List.of(first, second));
        String right = fingerprint(List.of(second, first));

        assertThat(left).isEqualTo(right).hasSize(64);
    }

    @Test
    void tenantSnapshotAndPayloadAreBoundIntoIdentity() {
        String baseline = fingerprint(List.of(new byte[]{1}));

        assertThat(WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-b",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        10L,
                        WRITE_BINDING,
                        "alice",
                        org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt()),
                List.of(new byte[]{1})))
                .isNotEqualTo(baseline);
        assertThat(WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        11L,
                        WRITE_BINDING,
                        "alice",
                        org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt()),
                List.of(new byte[]{1})))
                .isNotEqualTo(baseline);
        assertThat(WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        10L,
                        new byte[]{43},
                        "alice",
                        org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt()),
                List.of(new byte[]{1})))
                .isNotEqualTo(baseline);
        assertThat(fingerprint(List.of(new byte[]{2}))).isNotEqualTo(baseline);
    }

    @Test
    void entryRejectsFingerprintThatDoesNotMatchPayload() {
        assertThatThrownBy(() -> new WriteCommitEntry(
                "q1",
                "iceberg",
                "sales.orders",
                WriteCommitEntry.State.PENDING,
                1L,
                List.of(new byte[]{1}),
                "OVERWRITE",
                "0".repeat(64),
                "tenant-a",
                "alice",
                org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt(),
                10L,
                WRITE_BINDING,
                "0".repeat(64),
                null,
                null,
                0L,
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void entryKeepsAnImmutableCopyOfTheExactWorkerBinding() {
        byte[] binding = {42};
        List<byte[]> fragments = List.of(new byte[]{1});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        10L,
                        binding,
                        "alice",
                        org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt()),
                fragments);
        WriteCommitEntry entry = new WriteCommitEntry(
                "q1",
                "iceberg",
                "sales.orders",
                WriteCommitEntry.State.PENDING,
                1L,
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-a",
                "alice",
                org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt(),
                10L,
                binding,
                fingerprint,
                null,
                null,
                0L,
                null);

        binding[0] = 99;
        entry.writeOutputBinding()[0] = 88;

        assertThat(entry.writeOutputBinding()).containsExactly((byte) 42);
    }

    private static String fingerprint(List<byte[]> fragments) {
        return WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        10L,
                        WRITE_BINDING,
                        "alice",
                        org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt()),
                fragments);
    }

    @Test
    void publicationEvidenceCannotReplaceAnExistingIdentityOrChangeFingerprint() {
        var fragments = List.of(new byte[]{1});
        String fingerprint = fingerprint(fragments);
        var intent = new WriteCommitEntry(
                "q1",
                "iceberg",
                "sales.orders",
                WriteCommitEntry.State.PENDING,
                1L,
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-a",
                "alice",
                org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt(),
                10L,
                WRITE_BINDING,
                fingerprint,
                null,
                null,
                0L,
                null);
        var publication = new org.pangrova.spi.connector.metadata.NativeWriteFinalization(
                org.pangrova.spi.connector.metadata.NativeWriteFinalization.Outcome.COMMITTED, 11, new byte[32]);
        var published = intent.withPublication(publication);
        assertThat(published.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(published.withPublication(publication).publication()).isEqualTo(publication);
        assertThatThrownBy(() -> published.withPublication(
                new org.pangrova.spi.connector.metadata.NativeWriteFinalization(
                        publication.outcome(), 12, new byte[32])))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> intent.withPublication(
                new org.pangrova.spi.connector.metadata.NativeWriteFinalization(
                        org.pangrova.spi.connector.metadata.NativeWriteFinalization.Outcome.UNKNOWN, 0, new byte[0])))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> published.withState(WriteCommitEntry.State.ABORTED, 2L))
                .isInstanceOf(IllegalArgumentException.class);
        var aborted = intent.withState(WriteCommitEntry.State.ABORTED, 2L);
        assertThat(aborted.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(aborted.fragments()).singleElement().isEqualTo(new byte[]{1});
        assertThat(aborted.withState(WriteCommitEntry.State.ABORTED, 3L).state())
                .isEqualTo(WriteCommitEntry.State.ABORTED);
        assertThatThrownBy(() -> aborted.withState(WriteCommitEntry.State.PENDING, 3L))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> aborted.withState(WriteCommitEntry.State.COMMITTED, 3L))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> aborted.withPublication(publication))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
