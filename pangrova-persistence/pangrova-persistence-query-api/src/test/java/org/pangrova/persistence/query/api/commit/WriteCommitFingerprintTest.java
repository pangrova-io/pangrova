package org.pangrova.persistence.query.api.commit;

import org.pangrova.persistence.query.api.enums.WriteCommitState;


import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.common.file.CopyFileCommitBinding;

class WriteCommitFingerprintTest {

    private static org.pangrova.connector.api.commit.WriteAuthorizationReceipt receipt() {
        return new org.pangrova.connector.api.commit.WriteAuthorizationReceipt(new byte[]{1},
                new org.pangrova.connector.api.commit.WriteDeclarationSnapshot(
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("fixture", "schema", "table"),
                        "0".repeat(64), "0".repeat(64), false));
    }

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
    void ordinaryAndCopyWritesUseOneCanonicalFingerprintWithDistinctClaimPresence() {
        var fragments = List.of(new byte[]{1});
        String ordinary = insertFingerprint(null, fragments);
        var binding = new CopyFileCommitBinding("a".repeat(64), 0,
                "b".repeat(64), "00000000-0000-0000-0000-000000000001");
        String copy = insertFingerprint(binding, fragments);

        assertThat(copy).hasSize(64).isNotEqualTo(ordinary);
        assertThat(insertFingerprint(binding, fragments)).isEqualTo(copy);
        assertThat(insertFingerprint(new CopyFileCommitBinding(binding.fileKey(), 1,
                binding.claimDigest(), binding.tableUuid()), fragments)).isNotEqualTo(copy);
        assertThat(insertFingerprint(new CopyFileCommitBinding(binding.fileKey(), 0,
                "c".repeat(64), binding.tableUuid()), fragments)).isNotEqualTo(copy);
    }

    @Test
    void tenantSnapshotAndPayloadAreBoundIntoIdentity() {
        String baseline = fingerprint(List.of(new byte[]{1}));

        assertThat(WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-b",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        10L,
                        WRITE_BINDING,
                        "alice",
                        receipt(), null),
                List.of(new byte[]{1})))
                .isNotEqualTo(baseline);
        assertThat(WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        11L,
                        WRITE_BINDING,
                        "alice",
                        receipt(), null),
                List.of(new byte[]{1})))
                .isNotEqualTo(baseline);
        assertThat(WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        10L,
                        new byte[]{43},
                        "alice",
                        receipt(), null),
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
                WriteCommitState.PENDING,
                1L,
                List.of(new byte[]{1}),
                "OVERWRITE",
                "0".repeat(64),
                "tenant-a",
                "alice",
                receipt(),
                10L,
                WRITE_BINDING,
                "0".repeat(64),
                null,
                null,
                0L,
                null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void entryKeepsAnImmutableCopyOfTheExactWorkerBinding() {
        byte[] binding = {42};
        List<byte[]> fragments = List.of(new byte[]{1});
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        10L,
                        binding,
                        "alice",
                        receipt(), null),
                fragments);
        WriteCommitEntry entry = new WriteCommitEntry(
                "q1",
                "iceberg",
                "sales.orders",
                WriteCommitState.PENDING,
                1L,
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-a",
                "alice",
                receipt(),
                10L,
                binding,
                fingerprint,
                null,
                null,
                0L,
                null, null, null);

        binding[0] = 99;
        entry.writeOutputBinding()[0] = 88;

        assertThat(entry.writeOutputBinding()).containsExactly((byte) 42);
    }

    private static String fingerprint(List<byte[]> fragments) {
        return WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        "q1",
                        "tenant-a",
                        "iceberg",
                        "sales.orders",
                        "OVERWRITE",
                        10L,
                        WRITE_BINDING,
                        "alice",
                        receipt(), null),
                fragments);
    }

    private static String insertFingerprint(CopyFileCommitBinding binding, List<byte[]> fragments) {
        return WriteCommitFingerprint.compute(
                new WriteCommitFingerprint.Intent("q1", "tenant-a", "iceberg", "sales.orders",
                        "INSERT", 10L, WRITE_BINDING, "alice",
                        receipt(), binding), fragments);
    }

    @Test
    void cdcIdentityIsCanonicalAndBindsEverySourceCoordinate() {
        List<byte[]> fragments = List.of(new byte[]{1});
        CdcSourceIdentity.OffsetRange first = new CdcSourceIdentity.OffsetRange("orders", 0, 10, 20);
        CdcSourceIdentity.OffsetRange second = new CdcSourceIdentity.OffsetRange("orders", 1, 30, 40);
        CdcSourceIdentity reordered = new CdcSourceIdentity("stream-1", "epoch-1",
                List.of(second, first));
        CdcSourceIdentity canonical = new CdcSourceIdentity("stream-1", "epoch-1",
                List.of(first, second));

        String baseline = cdcFingerprint(canonical, fragments);
        assertThat(baseline).isEqualTo(cdcFingerprint(reordered, fragments))
                .isNotEqualTo(fingerprint(fragments));
        assertThat(cdcFingerprint(new CdcSourceIdentity("stream-2", "epoch-1",
                List.of(first)), fragments)).isNotEqualTo(baseline);
        assertThat(cdcFingerprint(new CdcSourceIdentity("stream-1", "epoch-2",
                List.of(first)), fragments)).isNotEqualTo(baseline);
        assertThat(cdcFingerprint(new CdcSourceIdentity("stream-1", "epoch-1",
                List.of(new CdcSourceIdentity.OffsetRange("orders", 0, 10, 21))), fragments))
                .isNotEqualTo(baseline);
        assertThat(reordered.ranges()).containsExactly(first, second);
    }

    @Test
    void cdcIdentityRejectsOverlappingRangesOnOnePartition() {
        CdcSourceIdentity.OffsetRange first = new CdcSourceIdentity.OffsetRange("orders", 0, 10, 20);
        CdcSourceIdentity.OffsetRange overlapping = new CdcSourceIdentity.OffsetRange("orders", 0, 19, 30);
        CdcSourceIdentity.OffsetRange adjacent = new CdcSourceIdentity.OffsetRange("orders", 0, 20, 30);

        assertThatThrownBy(() -> new CdcSourceIdentity("stream-1", "epoch-1",
                List.of(first, overlapping))).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> new CdcSourceIdentity("stream-1", "epoch-1",
                List.of(first, adjacent))).doesNotThrowAnyException();
    }

    private static String cdcFingerprint(CdcSourceIdentity cdcSource, List<byte[]> fragments) {
        return WriteCommitFingerprint.compute(
                new WriteCommitFingerprint.Intent("q1", "tenant-a", "iceberg", "sales.orders",
                        "OVERWRITE", 10L, WRITE_BINDING, "alice",
                        receipt(), null, cdcSource), fragments);
    }

    @Test
    void publicationEvidenceCannotReplaceAnExistingIdentityOrChangeFingerprint() {
        var fragments = List.of(new byte[]{1});
        String fingerprint = fingerprint(fragments);
        var intent = new WriteCommitEntry(
                "q1",
                "iceberg",
                "sales.orders",
                WriteCommitState.PENDING,
                1L,
                fragments,
                "OVERWRITE",
                fingerprint,
                "tenant-a",
                "alice",
                receipt(),
                10L,
                WRITE_BINDING,
                fingerprint,
                null,
                null,
                0L,
                null, null, null);
        var publication = new org.pangrova.connector.api.commit.NativeWriteFinalization(
                org.pangrova.connector.api.enums.NativeWriteCommitOutcome.COMMITTED, 11, new byte[32]);
        var published = intent.withPublication(publication);
        assertThat(published.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(published.withPublication(publication).publication()).isEqualTo(publication);
        assertThatThrownBy(() -> published.withPublication(
                new org.pangrova.connector.api.commit.NativeWriteFinalization(
                        publication.outcome(), 12, new byte[32])))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> intent.withPublication(
                new org.pangrova.connector.api.commit.NativeWriteFinalization(
                        org.pangrova.connector.api.enums.NativeWriteCommitOutcome.UNKNOWN, 0, new byte[0])))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> published.withState(WriteCommitState.ABORTED, 2L))
                .isInstanceOf(IllegalArgumentException.class);
        var aborted = intent.withState(WriteCommitState.ABORTED, 2L);
        assertThat(aborted.commitFingerprint()).isEqualTo(intent.commitFingerprint());
        assertThat(aborted.fragments()).singleElement().isEqualTo(new byte[]{1});
        assertThat(aborted.withState(WriteCommitState.ABORTED, 3L).state())
                .isEqualTo(WriteCommitState.ABORTED);
        assertThatThrownBy(() -> aborted.withState(WriteCommitState.PENDING, 3L))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> aborted.withState(WriteCommitState.COMMITTED, 3L))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> aborted.withPublication(publication))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
