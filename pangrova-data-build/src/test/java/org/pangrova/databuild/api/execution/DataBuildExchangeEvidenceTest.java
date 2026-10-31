package org.pangrova.databuild.api.execution;

import org.pangrova.databuild.protocol.DataBuildExchangeEvidenceCodec;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;

import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataBuildExchangeEvidenceTest {
    @Test
    void canonicalRoundTripPreservesExactRpcBytesAndHasNoCredentialFields() {
        var allocation = allocation();
        var finalization = finalized(allocation, List.of(partition(0)), 0);
        assertThat(DataBuildExchangeEvidenceCodec.decodeRegistration(DataBuildExchangeEvidenceCodec.encode(allocation), DataBuildExchangeEvidenceCodec.sha256(allocation))).isEqualTo(allocation);
        assertThat(DataBuildExchangeEvidenceCodec.decodeFinalization(DataBuildExchangeEvidenceCodec.encode(finalization), DataBuildExchangeEvidenceCodec.sha256(finalization))).isEqualTo(finalization);
        DataBuildExchangeEvidenceCodec.requireRegistration(finalization, allocation);
        assertThat(new String(DataBuildExchangeEvidenceCodec.encode(allocation), java.nio.charset.StandardCharsets.UTF_8))
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
        byte[] corrupted = DataBuildExchangeEvidenceCodec.encode(allocation);
        corrupted[corrupted.length - 1] ^= 1;
        assertThatThrownBy(() -> DataBuildExchangeEvidenceCodec.decodeRegistration(corrupted, DataBuildExchangeEvidenceCodec.sha256(allocation)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum");
    }

    @Test
    void exchangeDecodersRejectMalformedDigestsAndNoncanonicalBytesEvenWithCorrectDigest() {
        var allocation = allocation();
        var finalization = finalized(allocation, List.of(partition(0)), 0);
        for (String invalid : new String[]{null, "", "0".repeat(64), DataBuildExchangeEvidenceCodec.sha256(allocation).substring(1),
                DataBuildExchangeEvidenceCodec.sha256(allocation).toUpperCase(java.util.Locale.ROOT), " " + DataBuildExchangeEvidenceCodec.sha256(allocation)}) {
            assertThatThrownBy(() -> DataBuildExchangeEvidenceCodec.decodeRegistration(DataBuildExchangeEvidenceCodec.encode(allocation), invalid))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> DataBuildExchangeEvidenceCodec.decodeFinalization(DataBuildExchangeEvidenceCodec.encode(finalization), invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        byte[] canonical = DataBuildExchangeEvidenceCodec.encode(allocation);
        byte[] noncanonical = java.util.Arrays.copyOf(canonical, canonical.length + 1);
        noncanonical[canonical.length] = '\n';
        assertThatThrownBy(() -> DataBuildExchangeEvidenceCodec.decodeRegistration(noncanonical, DigestUtils.sha256Hex(noncanonical)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not canonical");
        assertThatThrownBy(() -> DataBuildExchangeEvidenceCodec.decodeRegistration(null, DataBuildExchangeEvidenceCodec.sha256(allocation)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("checksum or size");
    }

    @Test
    void finalizationRejectsResponseBytesWithWrongIntegrityDigest() {
        var allocation = allocation();
        // Negative metadata contract only: these bytes are not claimed to be a Native or shuffle receipt.
        String encoded = Base64.getEncoder().encodeToString(DataBuildExchangeEvidenceCodec.encode(allocation));
        assertThatThrownBy(() -> new DataBuildExchangeFinalization(allocation.exchangeId(), DataBuildExchangeEvidenceCodec.sha256(allocation),
                allocation.shuffleId(), allocation.planEpoch(), allocation.registeredOwner(),
                allocation.producerCount(), allocation.producerSetSha256(), List.of(partition(0)), 0, 0,
                encoded, "0".repeat(64)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("response checksum or size");
    }

    private static DataBuildExchangeRegistration allocation() {
        var address = new DataBuildExchangeRegistration.Address("shuffle", 9080, "shuffle");
        return new DataBuildExchangeRegistration("edge", "artifact:execution", "shuffle-actual", 1, 7,
                "a".repeat(64), "b".repeat(64), "41", 1, "c".repeat(64), 1,
                List.of(new DataBuildExchangeRegistration.Endpoint(address, 0, 1)), address,
                new DataBuildExecutionOwner("process", "incarnation", 3));
    }

    private static DataBuildExchangeFinalization.Partition partition(int id) {
        return new DataBuildExchangeFinalization.Partition(id, 0, 0, -1, "d".repeat(64));
    }

    private static DataBuildExchangeFinalization finalized(DataBuildExchangeRegistration allocation,
            List<DataBuildExchangeFinalization.Partition> partitions, long bytes) {
        byte[] response = new byte[]{8, 1};
        return new DataBuildExchangeFinalization(allocation.exchangeId(), DataBuildExchangeEvidenceCodec.sha256(allocation), allocation.shuffleId(),
                allocation.planEpoch(), allocation.registeredOwner(), 1, allocation.producerSetSha256(), partitions,
                bytes, 0, Base64.getEncoder().encodeToString(response), DigestUtils.sha256Hex(response));
    }
}
