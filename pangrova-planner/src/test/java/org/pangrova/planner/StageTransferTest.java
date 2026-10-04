package org.pangrova.planner;

import org.junit.jupiter.api.Test;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;
import org.pangrova.protocol.v2.BatchSerde;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StageTransferTest {
    @Test
    void bothProducerAndConsumerDeclarationsRequireTheOnlyCanonicalBatchFormat() {
        assertThat(new StageTransfer(1, 0, ShuffleType.SINGLETON, List.of(), "exchange", 1,
                BatchSerde.BATCH_SERDE_ARROW_BATCH, true).batchSerde()).isEqualTo(BatchSerde.BATCH_SERDE_ARROW_BATCH);
        assertThat(input(BatchSerde.BATCH_SERDE_ARROW_BATCH).batchSerde()).isEqualTo(BatchSerde.BATCH_SERDE_ARROW_BATCH);
        for (BatchSerde invalid : List.of(BatchSerde.BATCH_SERDE_UNSPECIFIED, BatchSerde.UNRECOGNIZED)) {
            assertThatThrownBy(() -> new StageTransfer(1, 0, ShuffleType.SINGLETON, List.of(), "exchange", 1,
                    invalid, true)).hasMessageContaining("canonical columnar");
            assertThatThrownBy(() -> input(invalid)).hasMessageContaining("canonical columnar");
        }
        assertThatThrownBy(() -> new StageTransfer(1, 0, ShuffleType.SINGLETON, List.of(), "exchange", 1,
                null, true)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> input(null)).isInstanceOf(NullPointerException.class);
    }

    private static PangrovaExchangeInput.Specification input(BatchSerde format) {
        return new PangrovaExchangeInput.Specification("node", "exchange", 1, 0,
                PangrovaDistribution.SINGLETON, 1, format, true);
    }
}
