package org.pangrova.metadata.artifact;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.metadata.artifact.build.ArtifactExchangeFinalization;
import org.pangrova.metadata.artifact.build.ArtifactExchangeRegistration;
import org.pangrova.metadata.artifact.build.ArtifactExecutionOwner;

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
