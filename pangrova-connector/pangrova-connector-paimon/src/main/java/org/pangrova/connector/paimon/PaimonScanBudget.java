package org.pangrova.connector.paimon;

import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/** Applies the stage deadline and caps on Paimon planning and split conversion. */
final class PaimonScanBudget {
    static final int MAX_METADATA_CANDIDATES = 1_000_000;

    private final SplitPlanningBudget stage;
    private final BooleanSupplier queryClosed;
    private final Thread owner = Thread.currentThread();
    private final int maxCandidates;

    PaimonScanBudget(SplitPlanningBudget stage, BooleanSupplier queryClosed, int maxCandidates) {
        this.stage = stage;
        this.queryClosed = queryClosed;
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

    List<Split> plan(ReadBuilder builder) {
        check(0);
        var scan = builder.newScan();
        var candidates = new AtomicLong();
        if (scan instanceof DataTableBatchScan batch) {
            // The SDK owns concurrent manifest reads. Finish that work before reporting a rejected
            // plan so a filter exception does not abandon the SDK's outstanding futures.
            batch.snapshotReader().withManifestEntryFilter(ignored ->
                    !cancelled() && !stage.expired() && candidates.incrementAndGet() <= maxCandidates);
        }
        var splits = scan.plan().splits();
        check(0);
        if (candidates.get() > maxCandidates) {
            throw metadataLimit("manifest entries");
        }
        return splits;
    }

    private boolean cancelled() {
        return owner.isInterrupted() || queryClosed.getAsBoolean();
    }

    private ConnectorException metadataLimit(String unit) {
        return new ConnectorException(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED,
                "Paimon planning exceeded " + maxCandidates + " candidate " + unit);
    }
}
