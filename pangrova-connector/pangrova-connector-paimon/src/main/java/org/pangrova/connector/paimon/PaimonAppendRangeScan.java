package org.pangrova.connector.paimon;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.IncrementalDeltaStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.table.SnapshotRange;
import org.pangrova.connector.paimon.scan.PaimonReadSchema;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reads and checks the exact SQL append interval without scheduling an SDK manifest batch. */
final class PaimonAppendRangeScan extends IncrementalDeltaStartingScanner {
    private final FileStoreTable table;
    private final Snapshot target;
    private final long fromExclusive;
    private final PaimonScanBudget budget;

    private PaimonAppendRangeScan(FileStoreTable table, Snapshot target, long fromExclusive, PaimonScanBudget budget) {
        super(table.snapshotManager(), fromExclusive, target.id(), ScanMode.DELTA);
        this.table = table;
        this.target = target;
        this.fromExclusive = fromExclusive;
        this.budget = budget;
    }

    static List<Split> plan(FileStoreTable table, Snapshot target, SnapshotRange range,
            Predicate filter, PaimonScanBudget budget) {
        if (!table.primaryKeys().isEmpty()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "Paimon CHANGES requires an append-only table without a primary key");
        }
        var options = table.coreOptions();
        var batch = new DataTableBatchScan(table.schema(), table.schemaManager(), options, table.newSnapshotReader(),
                table.catalogEnvironment().tableQueryAuth(options)) {
            @Override
            protected StartingScanner createStartingScanner(boolean streaming) {
                return new PaimonAppendRangeScan(table, target, range.fromInclusive(), budget);
            }
        };
        // Retain the SDK's authorization and data-evolution handling around the selected interval.
        DataTableScan scan = options.dataEvolutionEnabled() ? new DataEvolutionBatchScan(table, batch) : batch;
        budget.check(0);
        var splits = scan.withFilter(filter).plan().splits();
        budget.check(0);
        return splits;
    }

    @Override
    public Result scan(SnapshotReader reader) {
        budget.check(0);
        readSnapshot(fromExclusive);
        long[] candidates = {0};
        reader.withManifestEntryFilter(ignored -> budget.admitsCandidate(++candidates[0]));
        Map<Bucket, BucketFiles> filesByBucket = new LinkedHashMap<>();
        long manifestCount = 0;
        long fileCount = 0;
        for (long id = fromExclusive + 1; id <= target.id(); id++) {
            budget.check(fileCount);
            Snapshot snapshot = id == target.id() ? target : readSnapshot(id);
            if (snapshot.commitKind() != Snapshot.CommitKind.APPEND) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                        "Paimon changes require append commits, found " + snapshot.commitKind() + " at snapshot " + id);
            }
            var manifests = reader.manifestsReader().read(snapshot, ScanMode.DELTA).filteredManifests;
            manifestCount += manifests.size();
            budget.checkMetadataCount(manifestCount, "manifest files");
            for (var manifest : manifests) {
                budget.check(fileCount);
                List<ManifestEntry> entries = reader.readManifest(manifest);
                fileCount += entries.size();
                budget.check(fileCount);
                budget.checkMetadataCount(candidates[0], "manifest entries");
                for (ManifestEntry entry : entries) {
                    if (entry.kind() != FileKind.ADD) {
                        throw new IllegalStateException("Paimon append manifest contains a non-add file entry");
                    }
                    var bucket = new Bucket(entry.partition(), entry.bucket());
                    filesByBucket.computeIfAbsent(bucket,
                            ignored -> new BucketFiles(entry.totalBuckets())).files.add(entry.file());
                }
            }
        }
        return StartingScanner.fromPlan(new PlanImpl(null, target.id(), groupSplits(reader, filesByBucket)));
    }

    private Snapshot readSnapshot(long id) {
        try {
            return PaimonReadSchema.readSnapshot(table, id);
        } catch (FileNotFoundException missing) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SNAPSHOT_NOT_FOUND,
                    "Paimon append range has an unavailable snapshot " + id, missing);
        }
    }

    private List<Split> groupSplits(SnapshotReader reader, Map<Bucket, BucketFiles> filesByBucket) {
        List<Split> result = new ArrayList<>();
        for (var group : filesByBucket.entrySet()) {
            budget.check(0);
            var bucket = group.getKey();
            String path = reader.pathFactory().bucketPath(bucket.partition(), bucket.id()).toString();
            for (var split : reader.splitGenerator().splitForBatch(group.getValue().files)) {
                result.add(DataSplit.builder().withSnapshot(target.id()).isStreaming(true)
                        .withPartition(bucket.partition()).withBucket(bucket.id())
                        .withTotalBuckets(group.getValue().totalBuckets)
                        .withBucketPath(path).withDataFiles(split.files).rawConvertible(split.rawConvertible)
                        .build());
            }
        }
        budget.check(0);
        return result;
    }

    private record Bucket(BinaryRow partition, int id) { }

    private static final class BucketFiles {
        private final int totalBuckets;
        private final List<DataFileMeta> files = new ArrayList<>();

        private BucketFiles(int totalBuckets) {
            this.totalBuckets = totalBuckets;
        }
    }
}
