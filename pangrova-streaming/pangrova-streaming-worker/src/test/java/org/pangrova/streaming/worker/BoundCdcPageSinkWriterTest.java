package org.pangrova.streaming.worker;

import org.junit.jupiter.api.Test;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.data.ConnectorPageSink;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.enums.CdcOperation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class BoundCdcPageSinkWriterTest {

    @Test
    void usesTheExactBoundOutputHandleForDeleteAndInsertPhases() {
        FakeProvider provider = new FakeProvider();
        BoundCdcPageSinkWriter writer = new BoundCdcPageSinkWriter(
                provider, new Session(), provider.handle);

        List<byte[]> fragments = writer.writeBatch(List.of(
                envelope(CdcOperation.INSERT, null, row(1)),
                envelope(CdcOperation.DELETE, row(2), null),
                envelope(CdcOperation.UPDATE, row(3), row(4))));

        assertThat(provider.deleteHandles).containsExactly(provider.handle);
        assertThat(provider.insertHandles).containsExactly(provider.handle);
        assertThat(fragments).containsExactly(new byte[]{1}, new byte[]{2});
        assertThat(provider.deleteSink.pages).hasSize(1);
        assertThat(provider.deleteSink.pages.get(0).getPositionCount()).isEqualTo(2);
        assertThat(provider.insertSink.pages).hasSize(1);
        assertThat(provider.insertSink.pages.get(0).getPositionCount()).isEqualTo(2);
    }

    private static CdcEnvelope envelope(CdcOperation operation, CdcRow before, CdcRow after) {
        return new CdcEnvelope(operation, "target", before, after, Instant.EPOCH, 0, Map.of());
    }

    private static CdcRow row(int id) {
        return CdcRow.of(Map.of("id", id));
    }

    private static final class FakeProvider implements ConnectorPageSinkProvider {
        private final FakeHandle handle = new FakeHandle();
        private final FakeSink deleteSink = new FakeSink((byte) 1);
        private final FakeSink insertSink = new FakeSink((byte) 2);
        private final List<ConnectorOutputTableHandle> deleteHandles = new ArrayList<>();
        private final List<ConnectorOutputTableHandle> insertHandles = new ArrayList<>();

        @Override
        public ConnectorPageSink createPageSink(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorOutputTableHandle outputTableHandle) {
            insertHandles.add(outputTableHandle);
            return insertSink;
        }

        @Override
        public ConnectorPageSink createDeletePageSink(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorOutputTableHandle outputTableHandle) {
            deleteHandles.add(outputTableHandle);
            return deleteSink;
        }
    }

    private static final class FakeHandle implements ConnectorOutputTableHandle { }

    private static final class FakeSink implements ConnectorPageSink {
        private final byte fragment;
        private final List<Page> pages = new ArrayList<>();

        private FakeSink(byte fragment) {
            this.fragment = fragment;
        }

        @Override
        public void appendPage(Page page) {
            pages.add(page);
        }

        @Override
        public byte[] finishWithMetadata() {
            return new byte[]{fragment};
        }

        @Override
        public void abort() {
        }

        @Override
        public void close() {
        }
    }

    private static final class Session implements ConnectorSession {
        @Override
        public String getUser() {
            return "test-user";
        }

        @Override
        public Optional<String> getCatalog() {
            return Optional.of("catalog");
        }

        @Override
        public Optional<String> getSchema() {
            return Optional.of("schema");
        }

        @Override
        public Map<String, String> getProperties() {
            return Map.of("tenant_id", "tenant-a");
        }

        @Override
        public Optional<ConnectorQueryAuthority> getQueryAuthority() {
            return Optional.empty();
        }
    }
}
