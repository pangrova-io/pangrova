package org.pangrova.connector.paimon.handle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonDeletionVectorTest {
    @Test
    void exactMaximumByteEndIsRepresentable() {
        var vector = new PaimonDeletionVector("deletion.bin", Long.MAX_VALUE - 1, 1, 0L, 1);

        assertThat(vector.offset()).isEqualTo(Long.MAX_VALUE - 1);
        assertThat(vector.length()).isEqualTo(1);
    }

    @Test
    void byteEndOverflowRetainsArithmeticFailure() {
        assertThatThrownBy(() -> new PaimonDeletionVector("deletion.bin", Long.MAX_VALUE, 1, 0L, 1))
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> new PaimonDeletionVector("deletion.bin", 1, Long.MAX_VALUE, 0L, 1))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void invalidRangesAndCardinalitiesRemainRejected() {
        assertThatThrownBy(() -> new PaimonDeletionVector("deletion.bin", -1, 1, 0L, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PaimonDeletionVector("deletion.bin", 0, 0, 0L, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PaimonDeletionVector("deletion.bin", 0, 1, 2L, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
