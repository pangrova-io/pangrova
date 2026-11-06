package org.pangrova.shuffle.pangrova.client;

import org.junit.jupiter.api.Test;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.identity.ExchangeAttemptFence;
import org.pangrova.shuffle.api.identity.ExchangeChannelContext;
import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PangrovaPartitionReadTest {
    @Test
    void publicationChecksumIsImmutableAndHasContentEquality() {
        byte[] checksum = new byte[32];
        var request = new PangrovaPartitionRead("shuffle", context(), 2, 4, checksum);
        var same = new PangrovaPartitionRead("shuffle", context(), 2, 4, checksum.clone());
        checksum[0] = 1;
        request.expectedProducerSetChecksum()[1] = 2;
        assertThat(request.expectedProducerSetChecksum()).containsOnly((byte) 0);
        assertThat(request).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(request).isNotEqualTo(new PangrovaPartitionRead("shuffle", context(), 2, 4, checksum));
    }

    @Test
    void rejectsIncompletePublicationEvidence() {
        assertThatThrownBy(() -> new PangrovaPartitionRead(" ", context(), 2, 4, new byte[32]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PangrovaPartitionRead("s", null, 2, 4, new byte[32]))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new PangrovaPartitionRead("s", context(), 0, 4, new byte[32]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PangrovaPartitionRead("s", context(), 2, 0, new byte[32]))
                .isInstanceOf(IllegalArgumentException.class);
        for (byte[] invalid : new byte[][] {null, new byte[0], new byte[31], new byte[33]}) {
            assertThatThrownBy(() -> new PangrovaPartitionRead("s", context(), 2, 4, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static ExchangeChannelContext context() {
        return new ExchangeChannelContext(new ExchangeTaskAttempt("q", 2, 1, "task", 3,
                new ExchangeAttemptFence(2, 0, 1), new ShuffleExecutionOwner("owner", "incarnation", 1),
                "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"), 0, 100);
    }
}
