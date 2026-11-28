package org.pangrova.connector.paimon;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.DeleteFilter;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Real SDK commits plus explicit fault injection distinguish commit errors from capability rejection. */
class PaimonDeleteByFilterFailureTest extends PaimonDeleteFixture {

    private static final StoredRow EAST = new StoredRow(1, "east", "one", "target");
    private static final StoredRow WEST = new StoredRow(2, "west", "two", "retained");

    private final List<DeleteCommitProbe> openedCommits = new ArrayList<>();

    @ParameterizedTest(name = "partition={0}, afterCommit={1}, unsupported={2}")
    @CsvSource({"false,false,false", "false,false,true", "false,true,false", "false,true,true",
            "true,false,false", "true,false,true", "true,true,false", "true,true,true"})
    void commitFailuresAreWrappedAndClosedExactlyOnce(boolean partition, boolean afterCommit,
                                                       boolean unsupported) throws Exception {
        seedTable();
        long snapshot = snapshotId();
        RuntimeException failure = unsupported
                ? new UnsupportedOperationException("commit capability failure")
                : new UncheckedIOException("commit IO failure", new IOException("storage unavailable"));
        var table = guarded(new FailurePlan(failure, null, afterCommit, false));

        ConnectorException error = executeExpectingWriteError(table, filter(partition));

        assertThat(error).hasCause(failure);
        assertClosedOnce(1);
        if (afterCommit) {
            assertCommittedRows(partition);
        } else {
            assertThat(snapshotId()).isEqualTo(snapshot);
            assertThat(readRows()).containsExactlyInAnyOrder(EAST, WEST);
        }
    }

    @ParameterizedTest(name = "partition={0}, unsupportedClose={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void closeFailureAfterRealCommitIsNeverExposedAsCapabilityRejection(boolean partition,
                                                                      boolean unsupportedClose)
            throws Exception {
        seedTable();
        Exception failure = unsupportedClose
                ? new UnsupportedOperationException("close capability failure")
                : new IOException("close IO failure");
        var table = guarded(new FailurePlan(null, failure, false, false));

        ConnectorException error = executeExpectingWriteError(table, filter(partition));

        assertThat(error).hasCause(failure);
        assertClosedOnce(1);
        assertCommittedRows(partition);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void commitAndCloseFailuresPreserveOriginalCauseAndSuppressedCleanup(boolean partition) throws Exception {
        seedTable();
        long snapshot = snapshotId();
        var commitFailure = new UnsupportedOperationException("truncate failed");
        var closeFailure = new IOException("cleanup failed");
        var table = guarded(new FailurePlan(commitFailure, closeFailure, false, false));

        ConnectorException error = executeExpectingWriteError(table, filter(partition));

        assertThat(error).hasCause(commitFailure);
        assertThat(commitFailure.getSuppressed()).containsExactly(closeFailure);
        assertClosedOnce(1);
        assertThat(snapshotId()).isEqualTo(snapshot);
        assertThat(readRows()).containsExactlyInAnyOrder(EAST, WEST);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void successfulMetadataCommitClosesResource(boolean partition) throws Exception {
        seedTable();
        var table = guarded(new FailurePlan(null, null, false, false));

        PaimonDeleteByFilterExecutor.execute(table, filter(partition), DATABASE, TABLE_NAME);

        assertClosedOnce(1);
        assertCommittedRows(partition);
        DeleteCommitProbe commit = openedCommits.get(0);
        assertThat(commit.tableTruncations).isEqualTo(partition ? 0 : 1);
        assertThat(commit.partitionSpecs).isEqualTo(partition ? List.of(Map.of("region", "east")) : List.of());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void commitConstructionFailureIsWrappedBeforeAnyMutation(boolean partition) throws Exception {
        seedTable();
        long snapshot = snapshotId();
        var failure = new UnsupportedOperationException("newCommit failed");
        var table = new PaimonDeleteGuardTable(freshTable(), false, builder -> {
            throw failure;
        });

        ConnectorException error = executeExpectingWriteError(table, filter(partition));

        assertThat(error).hasCause(failure);
        assertThat(snapshotId()).isEqualTo(snapshot);
        assertThat(readRows()).containsExactlyInAnyOrder(EAST, WEST);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void actualSdkOneShotCommitFailureIsWrappedAndClosed(boolean appendOnly) throws Exception {
        createTable(true, appendOnly);
        writeRows(EAST, WEST);
        var table = guarded(new FailurePlan(null, null, false, true));

        ConnectorException error = executeExpectingWriteError(table, new DeleteFilter.All());

        // The second SDK truncate, not the test double, raises the one-shot commit exception.
        assertThat(error.getCause()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("BatchTableCommit only support one-time committing");
        assertClosedOnce(1);
        assertThat(readRows()).isEmpty();
    }

    private void seedTable() throws Exception {
        createTable(true, false);
        writeRows(EAST, WEST);
    }

    private PaimonDeleteGuardTable guarded(FailurePlan plan) throws Exception {
        return new PaimonDeleteGuardTable(freshTable(), false, builder -> {
            var commit = new DeleteCommitProbe(builder.newCommit(), plan);
            openedCommits.add(commit);
            return commit;
        });
    }

    private ConnectorException executeExpectingWriteError(Table table, DeleteFilter filter) {
        ConnectorException error = assertThrows(ConnectorException.class,
                () -> PaimonDeleteByFilterExecutor.execute(table, filter, DATABASE, TABLE_NAME));
        assertThat(error).isExactlyInstanceOf(ConnectorException.class)
                .hasMessageContaining("Failed to commit metadata DELETE")
                .hasMessageContaining(DATABASE + "." + TABLE_NAME);
        assertThat(error.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_WRITE_ERROR);
        return error;
    }

    private void assertClosedOnce(int truncations) {
        assertThat(openedCommits).hasSize(1);
        DeleteCommitProbe commit = openedCommits.get(0);
        assertThat(commit.truncations).isEqualTo(truncations);
        assertThat(commit.closeCalls).isEqualTo(1);
        assertThat(commit.delegateClosed).isTrue();
    }

    private void assertCommittedRows(boolean partition) throws Exception {
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(partition ? List.of(WEST) : List.of());
    }

    private static DeleteFilter filter(boolean partition) {
        return partition ? new DeleteFilter.Equal("region", "east") : new DeleteFilter.All();
    }

    private record FailurePlan(RuntimeException commitFailure, Exception closeFailure,
                               boolean afterCommit, boolean repeatSdkTruncate) {}

    private static final class DeleteCommitProbe implements BatchTableCommit {

        private final BatchTableCommit delegate;
        private final FailurePlan failure;
        private int truncations;
        private int tableTruncations;
        private int closeCalls;
        private boolean delegateClosed;
        private List<Map<String, String>> partitionSpecs = List.of();

        private DeleteCommitProbe(BatchTableCommit delegate, FailurePlan failure) {
            this.delegate = delegate;
            this.failure = failure;
        }

        @Override
        public void truncateTable() {
            tableTruncations++;
            truncate(() -> {
                delegate.truncateTable();
                if (failure.repeatSdkTruncate()) {
                    delegate.truncateTable();
                }
            });
        }

        @Override
        public void truncatePartitions(List<Map<String, String>> partitions) {
            partitionSpecs = partitions.stream().map(Map::copyOf).toList();
            truncate(() -> delegate.truncatePartitions(partitions));
        }

        private void truncate(Runnable operation) {
            truncations++;
            if (failure.afterCommit()) {
                operation.run();
            }
            if (failure.commitFailure() != null) {
                throw failure.commitFailure();
            }
            if (!failure.afterCommit()) {
                operation.run();
            }
        }

        @Override
        public void close() throws Exception {
            closeCalls++;
            delegate.close();
            delegateClosed = true;
            if (failure.closeFailure() != null) {
                throw failure.closeFailure();
            }
        }

        @Override
        public TableCommit withMetricRegistry(MetricRegistry registry) {
            delegate.withMetricRegistry(registry);
            return this;
        }

        @Override
        public void commit(List<CommitMessage> messages) {
            throw new AssertionError("Metadata DELETE must not commit rewritten data rows");
        }

        @Override
        public void abort(List<CommitMessage> messages) {
            throw new AssertionError("Metadata DELETE must not abort a row writer");
        }

        @Override
        public void updateStatistics(Statistics statistics) {
            throw new AssertionError("Metadata DELETE must not update statistics directly");
        }

        @Override
        public void compactManifests() {
            throw new AssertionError("Metadata DELETE must not request manifest compaction");
        }
    }
}