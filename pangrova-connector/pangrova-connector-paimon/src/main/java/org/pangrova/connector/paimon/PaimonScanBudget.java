package org.pangrova.connector.paimon;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.Split;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.api.query.ConnectorQueryOperation;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Applies the stage deadline and caps on Paimon planning and split conversion. */
final class PaimonScanBudget {
    static final int MAX_METADATA_CANDIDATES = 1_000_000;

    private final SplitPlanningBudget stage;
    private final BooleanSupplier queryClosed;
    private final Supplier<ConnectorQueryOperation> enterQueryOperation;
    private final Thread owner = Thread.currentThread();
    private final int maxCandidates;

    PaimonScanBudget(SplitPlanningBudget stage, BooleanSupplier queryClosed,
            Supplier<ConnectorQueryOperation> enterQueryOperation, int maxCandidates) {
        this.stage = stage;
        this.queryClosed = queryClosed;
        this.enterQueryOperation = enterQueryOperation;
        this.maxCandidates = maxCandidates;
    }

    void check(long splitCount) {
        if (cancelled()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                    "Paimon split enumeration was interrupted or its query closed");
        }
        stage.check(splitCount);
    }

    void checkSnapshotRange(long fromExclusive, long toInclusive) {
        check(0);
        if (fromExclusive < 0 || toInclusive < fromExclusive || toInclusive == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid Paimon incremental snapshot range");
        }
        checkMetadataCount(toInclusive - fromExclusive, "snapshots");
    }

    void checkMetadataCount(long count, String unit) {
        if (count > maxCandidates) throw metadataLimit(unit);
    }

    boolean admitsCandidate(long count) {
        return !cancelled() && !stage.expired() && count <= maxCandidates;
    }

    List<Split> plan(FileStoreTable table, Predicate filter, Snapshot snapshot) {
        return withPlanningTable(table, snapshot, (view, io) -> planSdk(view, filter, io));
    }

    List<Split> withPlanningTable(FileStoreTable table, Snapshot snapshot,
            BiFunction<FileStoreTable, PaimonPlanningFileIO, List<Split>> planner) {
        check(0);
        // SnapshotReader and the table's batch options are delegated to the selected SDK table.
        while (table instanceof DelegatedFileStoreTable delegated) table = delegated.wrapped();
        try (var io = new PaimonPlanningFileIO(table.fileIO(), this,
                enterQueryOperation.get(), stage.encodedMetadata())) {
            List<Split> splits;
            try {
                splits = planner.apply(planningTable(table, snapshot, io), io);
            } catch (RuntimeException failure) {
                if (io.rejection() != null) throw io.rejection();
                check(0);
                throw failure;
            }
            if (io.rejection() != null) throw io.rejection();
            check(0);
            return splits;
        }
    }

    private static FileStoreTable planningTable(FileStoreTable table, Snapshot snapshot, PaimonPlanningFileIO io) {
        FileStoreTable view;
        if (table instanceof AppendOnlyFileStoreTable) {
            view = new PaimonBoundedPlanningTable.Append(
                    io, table.location(), table.schema(), table.catalogEnvironment());
        } else if (table instanceof PrimaryKeyFileStoreTable) {
            view = new PaimonBoundedPlanningTable.PrimaryKey(
                    io, table.location(), table.schema(), table.catalogEnvironment());
        } else {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Paimon metadata planning requires an SDK physical table");
        }
        if (table.getManifestCache() != null) view.setManifestCache(table.getManifestCache());
        if (snapshot != null) {
            var verified = Caffeine.newBuilder().executor(Runnable::run).maximumSize(1).<Path, Snapshot>build();
            verified.put(view.snapshotManager().snapshotPath(snapshot.id()), snapshot);
            view.setSnapshotCache(verified);
        }
        return view;
    }

    private List<Split> planSdk(FileStoreTable table, Predicate filter, PaimonPlanningFileIO io) {
        var options = table.coreOptions();
        // Use the SDK's authorization, splitting and optional data-evolution logic on the guarded view.
        var batch = new DataTableBatchScan(table.schema(), table.schemaManager(), options, table.newSnapshotReader(),
                table.catalogEnvironment().tableQueryAuth(options));
        var candidates = new AtomicLong();
        batch.snapshotReader().withManifestEntryFilter(ignored -> {
            io.check();
            if (candidates.incrementAndGet() > maxCandidates) throw io.reject(metadataLimit("manifest entries"));
            return true;
        });
        DataTableScan scan = options.dataEvolutionEnabled() ? new DataEvolutionBatchScan(table, batch) : batch;
        return scan.withFilter(filter).plan().splits();
    }

    private boolean cancelled() {
        return owner.isInterrupted() || queryClosed.getAsBoolean();
    }

    private ConnectorException metadataLimit(String unit) {
        return new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED,
                "Paimon planning exceeded " + maxCandidates + " candidate " + unit);
    }
}
