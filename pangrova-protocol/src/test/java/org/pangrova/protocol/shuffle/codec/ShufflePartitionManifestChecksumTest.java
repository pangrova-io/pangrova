package org.pangrova.protocol.shuffle.codec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShufflePartitionManifestChecksumTest {

    @Test
    void producesCanonicalGoldenDigest() throws Exception {
        byte[] first = MessageDigest.getInstance("SHA-256").digest(
                "first".getBytes(StandardCharsets.UTF_8));
        byte[] second = MessageDigest.getInstance("SHA-256").digest(
                "second".getBytes(StandardCharsets.UTF_8));

        byte[] checksum = ShufflePartitionManifestChecksum.accumulator()
                .add(0, 5, 2, first)
                .add(1, 6, 3, second)
                .finish();

        assertThat(java.util.HexFormat.of().formatHex(checksum))
                .isEqualTo("dc255f32abf2317055758a6c2e4adb6bd8cd57d746592967bc9dd800ba256c2f");
    }

    @Test
    void rejectsSequenceGapsAndInvalidMetadata() throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(new byte[] {1});
        var accumulator = ShufflePartitionManifestChecksum.accumulator();

        assertThatThrownBy(() -> accumulator.add(1, 1, 1, digest))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.add(0, 0, 1, digest))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.add(0, 1, 0, digest))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> accumulator.add(0, 1, 1, new byte[31]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void supportsCanonicalEmptyAndRejectsRepeatedFinish() throws Exception {
        var accumulator = ShufflePartitionManifestChecksum.accumulator();
        assertThat(java.util.HexFormat.of().formatHex(accumulator.finish()))
                .isEqualTo("d251598ea374f6b94cc73b07a456080ac89d8c8fb59e7befbe2022aed2b5b6a0");
        assertThatThrownBy(accumulator::finish).isInstanceOf(IllegalStateException.class);

        var nonEmpty = ShufflePartitionManifestChecksum.accumulator();

        byte[] digest = MessageDigest.getInstance("SHA-256").digest(new byte[] {1});
        nonEmpty.add(0, 1, 1, digest).finishNonEmpty();

        assertThatThrownBy(nonEmpty::finish).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> nonEmpty.add(1, 1, 1, digest))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nonEmptyFinishRejectsEmptyProducerPartition() {
        assertThatThrownBy(() -> ShufflePartitionManifestChecksum.accumulator().finishNonEmpty())
                .isInstanceOf(IllegalStateException.class);
    }
}
