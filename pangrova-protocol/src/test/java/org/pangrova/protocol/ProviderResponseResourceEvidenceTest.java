package org.pangrova.protocol;

import org.pangrova.protocol.task.CanonicalTaskRequests;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.StageResourceGrant;

import static org.assertj.core.api.Assertions.assertThat;

class ProviderResponseResourceEvidenceTest {
    private static final StageResourceGrant GRANT = StageResourceGrant.newBuilder()
            .setProviderResponseBytes(8).build();

    @Test
    void rejectedDeliveredBytesDoNotInvalidateAdmittedRetentionEvidence() {
        assertThat(CanonicalTaskRequests.usageWithinGrant(usage(8, 9), GRANT)).isTrue();
        assertThat(CanonicalTaskRequests.usageWithinGrant(usage(8, Long.MAX_VALUE), GRANT)).isTrue();
    }

    @Test
    void retainedBytesStillCannotExceedTheGrant() {
        assertThat(CanonicalTaskRequests.usageWithinGrant(usage(9, 9), GRANT)).isFalse();
    }

    @Test
    void observedCounterCannotBeUnsignedOverflowOrLessThanAdmittedBytes() {
        assertThat(CanonicalTaskRequests.usageWithinGrant(usage(8, -1), GRANT)).isFalse();
        assertThat(CanonicalTaskRequests.usageWithinGrant(usage(8, 7), GRANT)).isFalse();
    }

    private static ResourceUsage usage(long admitted, long observed) {
        return ResourceUsage.newBuilder().setProviderResponseBytes(admitted)
                .setObservedProviderResponseBytes(observed).build();
    }
}
