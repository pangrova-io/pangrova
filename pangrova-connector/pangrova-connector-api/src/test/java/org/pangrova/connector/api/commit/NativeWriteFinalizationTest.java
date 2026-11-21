package org.pangrova.connector.api.commit;

import org.pangrova.connector.api.enums.NativeWriteCommitOutcome;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeWriteFinalizationTest {
    @Test
    void committedIdentityIsImmutableAndComparedByContent() {
        byte[] digest = new byte[32];
        digest[0] = 7;
        var result = new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 9, digest);
        digest[0] = 8;
        result.manifestSha256()[0] = 9;
        byte[] expected = new byte[32];
        expected[0] = 7;
        var same = new NativeWriteFinalization(NativeWriteCommitOutcome.COMMITTED, 9, expected);
        assertThat(result).isEqualTo(same);
        assertThat(result.hashCode()).isEqualTo(same.hashCode());
        assertThat(result.manifestSha256()).containsExactly(expected);
    }

    @Test
    void unconfirmedResultNeverCarriesPublicationEvidence() {
        for (var outcome : new NativeWriteCommitOutcome[]{
                NativeWriteCommitOutcome.UNKNOWN, NativeWriteCommitOutcome.NOT_COMMITTED}) {
            assertThat(new NativeWriteFinalization(outcome, 0, new byte[0]).publishedVersion()).isZero();
            assertThatThrownBy(() -> new NativeWriteFinalization(outcome, 1, new byte[32]))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new NativeWriteFinalization(
                NativeWriteCommitOutcome.COMMITTED, 0, new byte[32]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NativeWriteFinalization(
                NativeWriteCommitOutcome.COMMITTED, 1, new byte[31]))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
