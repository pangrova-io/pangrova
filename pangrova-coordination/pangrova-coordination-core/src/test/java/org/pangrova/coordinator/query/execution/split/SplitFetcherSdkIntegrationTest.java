package org.pangrova.coordinator.query.execution.split;

import org.pangrova.query.api.lifecycle.QueryState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.metadata.ReadBindingSdkFixture;
import org.pangrova.query.core.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.connector.spi.metadata.ConnectorMetadataScope;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Actual PG/plugin/SDK split-source component evidence; no Native task or distributed E2E claim. */
class SplitFetcherSdkIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void actualSdkEnumerationPreservesEveryPhysicalSplitAndClosesOnce() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            var table = fixture.create("rows");
            ReadBindingSdkFixture.write(table, 3);
            long snapshot = table.snapshotManager().latestSnapshotId();
            try (var scope = new QueryScope(fixture)) {
                var source = new ObservedSource(scope.openSource(), false);

                var splits = SplitFetcher.drainSource(source, 30_000);

                assertThat(splits).isNotEmpty().containsExactlyElementsOf(source.returnedSplits);
                assertThat(source.closeCount).isEqualTo(1);
                assertThat(source.finalBatch).isTrue();
                for (var split : splits) {
                    assertThat(split.dataFilePaths()).isNotEmpty();
                    var wire = DataSplit.parseFrom(split.toSplitInfoProto());
                    assertThat(wire.getConnectorId()).isEqualTo("paimon");
                    assertThat(wire.getSourceSnapshot()).isEqualTo(Long.toString(snapshot));
                }
            }
        }
    }

    @Test
    void actualSdkFinalPageInterruptionFailsInsteadOfReturningPhysicalSplits() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            ReadBindingSdkFixture.write(fixture.create("rows"), 3);
            try (var scope = new QueryScope(fixture)) {
                var source = new ObservedSource(scope.openSource(), true);
                try {
                    var failure = catchThrowable(() -> SplitFetcher.drainSource(source, 30_000));

                    assertThat(failure).isInstanceOf(PangrovaException.class)
                            .hasCauseInstanceOf(InterruptedException.class);
                    assertThat(((PangrovaException) failure).getErrorCode())
                            .isEqualTo(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED);
                    assertThat(Thread.currentThread().isInterrupted()).isTrue();
                    assertThat(source.returnedSplits).isNotEmpty();
                    assertThat(source.finalBatch).isTrue();
                    assertThat(source.closeCount).isEqualTo(1);
                } finally {
                    Thread.interrupted();
                }
            }
        }
    }

    private static final class QueryScope implements AutoCloseable {
        private final ReadBindingSdkFixture fixture;
        private final CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        private final QueryStateMachine state = new QueryStateMachine();
        private final ConnectorMetadataScope authority;

        QueryScope(ReadBindingSdkFixture fixture) {
            this.fixture = fixture;
            authority = state.bindConnectorAuthority(tracker.openQueryIssuance(fixture.context.queryId()));
        }

        ConnectorSplitSource openSource() {
            var actor = SessionContextConverter.toConnectorSession(
                    fixture.context.withQueryAuthority(authority), "bound", "db");
            try (var operation = authority.enterOperation()) {
                var catalog = fixture.bootstrap.catalogRegistry().getCatalog("bound");
                var metadata = authority.beginMetadata("bound", () -> catalog.getMetadata(actor));
                var handle = metadata.getTableHandle(actor, new SchemaTableName("db", "rows"));
                assertThat(handle).isNotNull();
                var binding = metadata.bindRead(actor, handle);
                return catalog.getSplitManager().getSplits(ConnectorTransactionHandle.empty(), actor,
                        binding.handle(), DynamicFilter.ALL, Constraint.EMPTY);
            }
        }

        @Override
        public void close() {
            state.tryTransition(QueryState.CANCELLED);
            tracker.sealQueryIssuance(fixture.context.queryId());
            authority.cleanupMetadata().requireSuccess();
            tracker.retireQueryIssuance(fixture.context.queryId());
        }
    }

    /** Observes the original SDK-backed source and injects only deterministic thread interruption. */
    private static final class ObservedSource implements ConnectorSplitSource {
        private final ConnectorSplitSource actual;
        private final boolean interruptAfterBatch;
        private List<ConnectorSplit> returnedSplits = List.of();
        private boolean finalBatch;
        private int closeCount;

        ObservedSource(ConnectorSplitSource actual, boolean interruptAfterBatch) {
            this.actual = actual;
            this.interruptAfterBatch = interruptAfterBatch;
        }

        @Override
        public List<ConnectorSplit> getNextBatch(int maxSize) {
            returnedSplits = actual.getNextBatch(maxSize);
            finalBatch = actual.isFinished();
            if (interruptAfterBatch) Thread.currentThread().interrupt();
            return returnedSplits;
        }

        @Override
        public boolean isFinished() {
            return actual.isFinished();
        }

        @Override
        public void close() {
            closeCount++;
            actual.close();
        }
    }
}
