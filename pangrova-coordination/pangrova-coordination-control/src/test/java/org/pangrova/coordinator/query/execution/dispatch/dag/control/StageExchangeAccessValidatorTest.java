package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.exchange.GenerationExchangeFinalization;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.protocol.v2.ExternalShuffleInput;
import org.pangrova.protocol.v2.ShufflePartitionPublication;

import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StageExchangeAccessValidatorTest {
    private static final GenerationExchangeFinalization RECEIPT = new GenerationExchangeFinalization(
            "edge", "a".repeat(64), "allocation", 7, new GenerationExecutionOwner("owner", "process", 13),
            4, "b".repeat(64), List.of(
                    new GenerationExchangeFinalization.Partition(0, 0, 0, -1, "c".repeat(64)),
                    new GenerationExchangeFinalization.Partition(1, 751, 23, 3, "d".repeat(64))),
            751, 23, "e".repeat(64));

    @Test
    void matchesBothSinglePartitionAndContiguousRangeToTheDurableReceipt() {
        assertThatCode(() -> StageExchangeAccessValidator.requirePublishedRange(
                input(1, publication(1)), RECEIPT)).doesNotThrowAnyException();
        assertThatCode(() -> StageExchangeAccessValidator.requirePublishedRange(
                input(0, publication(0), publication(1)), RECEIPT)).doesNotThrowAnyException();
    }

    @Test
    void rejectsWrongManifestSequenceCountersAndAnImplicitEmptyPartition() {
        var correct = publication(1);
        var forged = List.of(
                correct.toBuilder().setManifestSha256(publication(0).getManifestSha256()).build(),
                correct.toBuilder().setLastSequence(2).build(),
                correct.toBuilder().setTotalBytes(750).build(),
                correct.toBuilder().setTotalRows(24).build());
        for (var publication : forged) {
            assertThatThrownBy(() -> StageExchangeAccessValidator.requirePublishedRange(
                    input(1, publication), RECEIPT)).hasMessageContaining("differs from the persisted");
        }
        assertThatThrownBy(() -> StageExchangeAccessValidator.requirePublishedRange(
                input(0, publication(0).toBuilder().clearTotalRows().build()), RECEIPT))
                .hasMessageContaining("differs from the persisted");
    }

    @Test
    void rejectsRangeWhoseEndExceedsDurablePartitionCount() {
        assertThatThrownBy(() -> StageExchangeAccessValidator.requirePublishedRange(
                input(1, publication(1), publication(0)), RECEIPT)).hasMessageContaining("outside");
    }

    private static ExternalShuffleInput input(int start, ShufflePartitionPublication... publications) {
        return ExternalShuffleInput.newBuilder().setSourcePartitionStart(start)
                .addAllPublications(List.of(publications)).build();
    }

    private static ShufflePartitionPublication publication(int partition) {
        var value = RECEIPT.partitions().get(partition);
        return ShufflePartitionPublication.newBuilder()
                .setLastSequence(value.committedLastSequence()).setTotalBytes(value.totalBytes())
                .setTotalRows(value.totalRows())
                .setManifestSha256(ByteString.copyFrom(HexFormat.of().parseHex(value.manifestSha256())))
                .build();
    }
}
