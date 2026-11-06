package org.pangrova.connector.api.commit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeWriteFinalizationTest {
    @Test
    void committedIdentityIsImmutableAndComparedByContent() {
        byte[] digest = new byte[32];
        digest[0] = 7;
        var result = new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 9, digest);
        digest[0] = 8;
        result.manifestSha256()[0] = 9;
        byte[] expected = new byte[32];
        expected[0] = 7;
        var same = new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 9, expected);
        assertThat(result).isEqualTo(same);
        assertThat(result.hashCode()).isEqualTo(same.hashCode());
        assertThat(result.manifestSha256()).containsExactly(expected);
    }

    @Test
    void unconfirmedResultNeverCarriesPublicationEvidence() {
        for (var outcome : new NativeWriteFinalization.Outcome[]{
                NativeWriteFinalization.Outcome.UNKNOWN, NativeWriteFinalization.Outcome.NOT_COMMITTED}) {
            assertThat(new NativeWriteFinalization(outcome, 0, new byte[0]).publishedVersion()).isZero();
            assertThatThrownBy(() -> new NativeWriteFinalization(outcome, 1, new byte[32]))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new NativeWriteFinalization(
                NativeWriteFinalization.Outcome.COMMITTED, 0, new byte[32]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NativeWriteFinalization(
                NativeWriteFinalization.Outcome.COMMITTED, 1, new byte[31]))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
