package org.pangrova.indexing.artifact;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.indexing.control.ArtifactExchangeFinalization;
import org.pangrova.indexing.control.ArtifactExchangeRegistration;
import org.pangrova.indexing.control.ArtifactExecutionOwner;

import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactExchangeEvidenceTest {
    @Test
    void canonicalRoundTripPreservesExactRpcBytesAndHasNoCredentialFields() {
        var allocation = allocation();
        var finalization = finalized(allocation, List.of(partition(0)), 0);
        assertThat(ArtifactExchangeRegistration.decode(allocation.canonicalBytes(), allocation.sha256())).isEqualTo(allocation);
        assertThat(ArtifactExchangeFinalization.decode(finalization.canonicalBytes(), finalization.sha256())).isEqualTo(finalization);
        finalization.requireRegistration(allocation);
        assertThat(new String(allocation.canonicalBytes(), java.nio.charset.StandardCharsets.UTF_8))
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
        byte[] corrupted = allocation.canonicalBytes();
        corrupted[corrupted.length - 1] ^= 1;
        assertThatThrownBy(() -> ArtifactExchangeRegistration.decode(corrupted, allocation.sha256()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
    }

    @Test
    void exchangeDecodersRejectMalformedDigestsAndNoncanonicalBytesEvenWithCorrectDigest() {
        var allocation = allocation();
        var finalization = finalized(allocation, List.of(partition(0)), 0);
        for (String invalid : new String[]{null, "", "0".repeat(64), allocation.sha256().substring(1),
                allocation.sha256().toUpperCase(java.util.Locale.ROOT), " " + allocation.sha256()}) {
            assertThatThrownBy(() -> ArtifactExchangeRegistration.decode(allocation.canonicalBytes(), invalid))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ArtifactExchangeFinalization.decode(finalization.canonicalBytes(), invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        byte[] canonical = allocation.canonicalBytes();
        byte[] noncanonical = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        noncanonical[canonical.length] = '\n';
        assertThatThrownBy(() -> ArtifactExchangeRegistration.decode(noncanonical, DigestUtils.sha256Hex(noncanonical)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not canonical");
        assertThatThrownBy(() -> ArtifactExchangeRegistration.decode(null, allocation.sha256()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum or size");
    }

    @Test
    void finalizationRejectsResponseBytesWithWrongIntegrityDigest() {
        var allocation = allocation();
        // Negative metadata contract only: these bytes are not claimed to be a Native or shuffle receipt.
        String encoded = Base64.getEncoder().encodeToString(allocation.canonicalBytes());
        assertThatThrownBy(() -> new ArtifactExchangeFinalization(allocation.exchangeId(), allocation.sha256(),
                allocation.shuffleId(), allocation.planEpoch(), allocation.registeredOwner(),
                allocation.producerCount(), allocation.producerSetSha256(), List.of(partition(0)), 0, 0,
                encoded, "0".repeat(64)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("response checksum or size");
    }

    private static ArtifactExchangeRegistration allocation() {
        var address = new ArtifactExchangeRegistration.Address("shuffle", 9080, "shuffle");
        return new ArtifactExchangeRegistration("edge", "artifact:execution", "shuffle-actual", 1, 7,
                "a".repeat(64), "b".repeat(64), "41", 1, "c".repeat(64), 1,
                List.of(new ArtifactExchangeRegistration.Endpoint(address, 0, 1)), address,
                new ArtifactExecutionOwner("process", "incarnation", 3));
    }

    private static ArtifactExchangeFinalization.Partition partition(int id) {
        return new ArtifactExchangeFinalization.Partition(id, 0, 0, -1, "d".repeat(64));
    }

    private static ArtifactExchangeFinalization finalized(ArtifactExchangeRegistration allocation,
            List<ArtifactExchangeFinalization.Partition> partitions, long bytes) {
        byte[] response = new byte[]{8, 1};
        return new ArtifactExchangeFinalization(allocation.exchangeId(), allocation.sha256(), allocation.shuffleId(),
                allocation.planEpoch(), allocation.registeredOwner(), 1, allocation.producerSetSha256(), partitions,
                bytes, 0, Base64.getEncoder().encodeToString(response), DigestUtils.sha256Hex(response));
    }
}
