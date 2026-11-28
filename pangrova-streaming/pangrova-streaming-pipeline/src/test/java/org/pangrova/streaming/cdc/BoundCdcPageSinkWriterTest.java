package org.pangrova.streaming.cdc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.data.ConnectorCdcPageSink;
import org.pangrova.connector.api.data.ConnectorPageSink;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.enums.CdcRowKind;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.enums.CdcOperation;
import org.pangrova.types.value.TypedRow;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundCdcPageSinkWriterTest {

    @Test
    void appendsThroughTheExactOutputHandleWhenTheConnectorHasNoCdcSink() {
        FakeProvider provider = new FakeProvider();
        BoundCdcPageSinkWriter writer = new BoundCdcPageSinkWriter(provider, new Session(), provider.handle);

        List<byte[]> fragments = writer.writeBatch(List.of(envelope(CdcOperation.INSERT, null, row(1)),
                envelope(CdcOperation.INSERT, null, row(2))));

        assertThat(provider.insertHandles).containsExactly(provider.handle);
        assertThat(fragments).containsExactly(new byte[]{2});
        assertThat(provider.insertSink.pages).singleElement().satisfies(page ->
                assertThat(page.getPositionCount()).isEqualTo(2));
    }

    @ParameterizedTest
    @EnumSource(value = CdcOperation.class, names = {"UPDATE", "DELETE"})
    void rejectsMutationsBeforeAnOverwriteOrPartialAppendCanBeStaged(CdcOperation operation) {
        FakeProvider provider = new FakeProvider();
        BoundCdcPageSinkWriter writer = new BoundCdcPageSinkWriter(provider, new Session(), provider.handle);
        CdcRow after = operation == CdcOperation.UPDATE ? row(3) : null;

        assertThatThrownBy(() -> writer.writeBatch(List.of(envelope(CdcOperation.INSERT, null, row(1)),
                envelope(operation, row(2), after))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("atomic CDC UPDATE/DELETE");
        assertThat(provider.insertHandles).isEmpty();
    }

    @Test
    void stagesOneFragmentInSourceOrderUsingTheExactOutputHandle() {
        var provider = new FakeProvider();
        provider.cdcSink = new RecordingCdcSink();
        var writer = new BoundCdcPageSinkWriter(provider, new Session(), provider.handle);

        var fragments = writer.writeBatch(List.of(
                envelope(CdcOperation.INSERT, null, row(1)),
                envelope(CdcOperation.UPDATE, row(1), row(2)),
                envelope(CdcOperation.DELETE, row(3), null)));

        assertThat(provider.cdcHandles).containsExactly(provider.handle);
        assertThat(provider.insertHandles).isEmpty();
        assertThat(provider.cdcSink.rows).containsExactly(new Mutation(CdcRowKind.INSERT, 1),
                new Mutation(CdcRowKind.UPDATE_BEFORE, 1), new Mutation(CdcRowKind.UPDATE_AFTER, 2),
                new Mutation(CdcRowKind.DELETE, 3));
        assertThat(fragments).containsExactly(new byte[]{4});
        assertThat(provider.cdcSink.finished).isTrue();
        assertThat(provider.cdcSink.closed).isTrue();
        assertThat(provider.cdcSink.aborted).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = CdcOperation.class, names = {"INSERT", "UPDATE", "DELETE"})
    void missingImageAbortsTheSingleStagingSinkWithoutReturningAFragment(CdcOperation operation) {
        assertMissingImage(envelope(operation, null, null), operation == CdcOperation.INSERT ? "after" : "before");
    }

    @Test
    void missingUpdateAfterImageAlsoDiscardsTheStagedBeforeImage() {
        assertMissingImage(envelope(CdcOperation.UPDATE, row(2), null), "after");
    }

    private static void assertMissingImage(CdcEnvelope invalid, String image) {
        var provider = new FakeProvider();
        provider.cdcSink = new RecordingCdcSink();
        var writer = new BoundCdcPageSinkWriter(provider, new Session(), provider.handle);

        assertThatThrownBy(() -> writer.writeBatch(List.of(envelope(CdcOperation.INSERT, null, row(1)), invalid)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("no " + image + " image");
        assertThat(provider.cdcHandles).containsExactly(provider.handle);
        assertThat(provider.insertHandles).isEmpty();
        assertThat(provider.cdcSink.finished).isFalse();
        assertThat(provider.cdcSink.aborted).isTrue();
        assertThat(provider.cdcSink.closed).isTrue();
        assertThat(provider.cdcSink.rows).isEmpty();
    }

    private record Mutation(CdcRowKind kind, Object id) {}

    private static final class RecordingCdcSink implements ConnectorCdcPageSink {
        private final List<Mutation> rows = new ArrayList<>();
        private boolean finished;
        private boolean aborted;
        private boolean closed;

        @Override
        public void appendRow(TypedRow row, CdcRowKind kind) {
            rows.add(new Mutation(kind, row.getRaw("id")));
        }

        @Override
        public void appendPage(Page page) {
            throw new AssertionError("CDC staging requires a row kind");
        }

        @Override
        public byte[] finishWithMetadata() {
            finished = true;
            return new byte[]{(byte) rows.size()};
        }

        @Override
        public void abort() {
            aborted = true;
            rows.clear();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static CdcEnvelope envelope(CdcOperation operation, CdcRow before, CdcRow after) {
        return new CdcEnvelope(operation, "target", before, after, Instant.EPOCH, 0, Map.of());
    }

    private static CdcRow row(int id) {
        return CdcRow.of(Map.of("id", id));
    }

    private static final class FakeProvider implements ConnectorPageSinkProvider {
        private final FakeHandle handle = new FakeHandle();
        private final FakeSink insertSink = new FakeSink((byte) 2);
        private final List<ConnectorOutputTableHandle> insertHandles = new ArrayList<>();
        private final List<ConnectorOutputTableHandle> cdcHandles = new ArrayList<>();
        private RecordingCdcSink cdcSink;

        @Override
        public ConnectorCdcPageSink createCdcPageSink(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                ConnectorOutputTableHandle outputTableHandle) {
            if (cdcSink == null) throw new UnsupportedOperationException();
            cdcHandles.add(outputTableHandle);
            return cdcSink;
        }

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
            throw new AssertionError("CDC UPDATE/DELETE cannot use the relational delete sink");
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
