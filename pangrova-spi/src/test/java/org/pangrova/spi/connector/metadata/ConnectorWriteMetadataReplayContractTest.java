package org.pangrova.spi.connector.metadata;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.base.ConnectorException;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorWriteMetadataReplayContractTest {

    @Test
    void journaledCommitNeverFallsBackToContextFreeCommit() {
        AtomicInteger contextFreeCalls = new AtomicInteger();
        ConnectorWriteMetadata metadata = new ConnectorWriteMetadata() {
            @Override
            public void finishDistributedWrite(
                    ConnectorSession session, ConnectorOutputTableHandle handle,
                    List<byte[]> fragments) {
                contextFreeCalls.incrementAndGet();
            }
        };
        DistributedWriteCommitContext context = new DistributedWriteCommitContext(
                "query-1",
                "tenant-1",
                -1L,
                new byte[]{1},
                "a".repeat(64),
                "alice",
                org.pangrova.spi.coordination.SpiWriteReceiptFixtures.receipt(), null);

        assertThatThrownBy(() -> metadata.finishDistributedWrite(
                null, null, List.of(), context))
                .isInstanceOf(ConnectorException.class)
                .hasMessageContaining("Replay-safe distributed write commit");
        assertThat(contextFreeCalls).hasValue(0);
    }
}
