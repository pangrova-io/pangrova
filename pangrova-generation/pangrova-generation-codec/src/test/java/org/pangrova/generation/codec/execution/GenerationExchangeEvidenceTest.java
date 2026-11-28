package org.pangrova.generation.codec.execution;

import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;
import org.pangrova.generation.api.exchange.GenerationExchangeRegistration;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;

import org.pangrova.generation.codec.execution.GenerationExchangeEvidenceCodec;

import org.junit.jupiter.api.Test;
import org.pangrova.common.crypto.Sha256Digests;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerationExchangeEvidenceTest {
    @Test
    void canonicalRoundTripPreservesProviderDigestAndHasNoCredentialFields() {
        var allocation = allocation();
        var finalization = finalized(allocation, List.of(partition(0)), 0);
        assertThat(GenerationExchangeEvidenceCodec.decodeRegistration(GenerationExchangeEvidenceCodec.encode(allocation), GenerationExchangeEvidenceCodec.sha256(allocation))).isEqualTo(allocation);
        assertThat(GenerationExchangeEvidenceCodec.decodeFinalization(GenerationExchangeEvidenceCodec.encode(finalization), GenerationExchangeEvidenceCodec.sha256(finalization))).isEqualTo(finalization);
        GenerationExchangeEvidenceCodec.requireRegistration(finalization, allocation);
        assertThat(new String(GenerationExchangeEvidenceCodec.encode(allocation), java.nio.charset.StandardCharsets.UTF_8))
                .doesNotContain("capability", "securityToken", "accessKey", "password");
    }

    @Test
    void missingDuplicateAndNoncontiguousPartitionsCannotBeFinalized() {
        var allocation = allocation();
        assertThatThrownBy(() -> finalized(allocation, List.of(), 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> finalized(allocation, List.of(partition(0), partition(0)), 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cohort");
        assertThatThrownBy(() -> finalized(allocation, List.of(partition(1)), 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cohort");
        assertThatThrownBy(() -> finalized(allocation, List.of(partition(0)), 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("totals");
    }

    @Test
    void allocationChecksumTamperingIsRejectedBeforeReplay() {
        var allocation = allocation();
        byte[] corrupted = GenerationExchangeEvidenceCodec.encode(allocation);
        corrupted[corrupted.length - 1] ^= 1;
        assertThatThrownBy(() -> GenerationExchangeEvidenceCodec.decodeRegistration(corrupted, GenerationExchangeEvidenceCodec.sha256(allocation)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
    }

    @Test
    void exchangeDecodersRejectMalformedDigestsAndNoncanonicalBytesEvenWithCorrectDigest() {
        var allocation = allocation();
        var finalization = finalized(allocation, List.of(partition(0)), 0);
        for (String invalid : new String[]{null, "", "0".repeat(64), GenerationExchangeEvidenceCodec.sha256(allocation).substring(1),
                GenerationExchangeEvidenceCodec.sha256(allocation).toUpperCase(java.util.Locale.ROOT), " " + GenerationExchangeEvidenceCodec.sha256(allocation)}) {
            assertThatThrownBy(() -> GenerationExchangeEvidenceCodec.decodeRegistration(GenerationExchangeEvidenceCodec.encode(allocation), invalid))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> GenerationExchangeEvidenceCodec.decodeFinalization(GenerationExchangeEvidenceCodec.encode(finalization), invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        byte[] receipt = GenerationExchangeEvidenceCodec.encode(finalization);
        assertThat(new String(receipt, java.nio.charset.StandardCharsets.UTF_8))
                .contains(finalization.responseSha256()).doesNotContain("responseBase64");
        byte[] canonical = GenerationExchangeEvidenceCodec.encode(allocation);
        byte[] noncanonical = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        noncanonical[canonical.length] = '\n';
        assertThatThrownBy(() -> GenerationExchangeEvidenceCodec.decodeRegistration(noncanonical, Sha256Digests.sha256Hex(noncanonical)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not canonical");
        assertThatThrownBy(() -> GenerationExchangeEvidenceCodec.decodeRegistration(null, GenerationExchangeEvidenceCodec.sha256(allocation)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum or size");
    }

    private static GenerationExchangeRegistration allocation() {
        return new GenerationExchangeRegistration("edge", "artifact:execution", "shuffle-actual", 1, 7,
                "a".repeat(64), "b".repeat(64), "41", 1, "c".repeat(64), 1,
                ShuffleProviderDescriptor.of("test", 1, new byte[]{1, 2, 3}),
                new GenerationExecutionOwner("process", "incarnation", 3));
    }

    private static GenerationExchangeFinalization.Partition partition(int id) {
        return new GenerationExchangeFinalization.Partition(id, 0, 0, -1, "d".repeat(64));
    }

    private static GenerationExchangeFinalization finalized(GenerationExchangeRegistration allocation,
            List<GenerationExchangeFinalization.Partition> partitions, long bytes) {
        byte[] response = new byte[]{8, 1};
        return new GenerationExchangeFinalization(allocation.exchangeId(), GenerationExchangeEvidenceCodec.sha256(allocation), allocation.shuffleId(),
                allocation.planEpoch(), allocation.registeredOwner(), 1, allocation.producerSetSha256(), partitions,
                bytes, 0, Sha256Digests.sha256Hex(response));
    }
}
