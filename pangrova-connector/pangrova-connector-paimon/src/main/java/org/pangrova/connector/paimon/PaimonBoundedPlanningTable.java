package org.pangrova.connector.paimon;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.BucketSelectConverter;
import org.apache.paimon.operation.DataEvolutionFileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.SnapshotReaderImpl;
import org.apache.paimon.utils.ManifestReadThreadPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/** SDK planning views with bounded manifest submission and the original split generators. */
final class PaimonBoundedPlanningTable {
    private static final int MAX_PENDING_MANIFESTS = 32;

    private PaimonBoundedPlanningTable() {}

    private static Iterator<ManifestEntry> readEntries(List<ManifestFileMeta> manifests, ScanMode mode,
            Integer parallelism, PaimonPlanningFileIO io, Function<ManifestFileMeta, List<ManifestEntry>> read) {
        // The Paimon executor allocates one Future per input, even with a semaphore.
        int window = Math.max(1, Math.min(MAX_PENDING_MANIFESTS,
                parallelism == null ? Runtime.getRuntime().availableProcessors() : parallelism));
        var entries = new ArrayList<ManifestEntry>();
        var deletes = new HashSet<FileEntry.Identifier>();
        for (int start = 0; start < manifests.size(); start += window) {
            io.check();
            var batch = manifests.subList(start, Math.min(start + window, manifests.size()));
            ManifestReadThreadPool.randomlyExecuteSequentialReturn(read, batch, window).forEachRemaining(entry -> {
                if (mode == ScanMode.ALL && entry.kind() == FileKind.DELETE) {
                    deletes.add(entry.identifier());
                } else {
                    entries.add(entry);
                }
            });
        }
        // A delete can be in a later window and still removes an add from an earlier window.
        if (!deletes.isEmpty()) entries.removeIf(entry -> deletes.contains(entry.identifier()));
        return entries.iterator();
    }

    private static ManifestsReader manifestsReader(FileStoreTable table) {
        var store = table.store();
        return new ManifestsReader(store.partitionType(), table.coreOptions().partitionDefaultName(),
                table.snapshotManager(), store.manifestListFactory());
    }

    static final class Append extends AppendOnlyFileStoreTable {
        private static final long serialVersionUID = 1L;
        private final transient PaimonPlanningFileIO planningIo;

        Append(PaimonPlanningFileIO io, Path path, TableSchema schema, CatalogEnvironment environment) {
            super(io, path, schema, environment);
            this.planningIo = io;
        }

        @Override
        public SnapshotReader newSnapshotReader() {
            var store = store();
            var schema = schema();
            var options = coreOptions();
            var buckets = new BucketSelectConverter(store.bucketMode(), options.bucketFunctionType(),
                    schema.logicalRowType().notNull(), schema.logicalPartitionType(), schema.logicalBucketKeyType());
            var manifests = manifestsReader(this);
            var manifestFiles = store.manifestFileFactory();
            AppendOnlyFileStoreScan scan;
            if (options.dataEvolutionEnabled()) {
                scan = new DataEvolutionFileStoreScan(manifests, buckets, snapshotManager(), schemaManager(), schema,
                        manifestFiles, options.scanManifestParallelism(), options.deletionVectorsEnabled()) {
                    @Override
                    public Iterator<ManifestEntry> readManifestEntries(List<ManifestFileMeta> files, boolean sequential) {
                        return readEntries(files, scanMode, parallelism(), planningIo, this::readManifest);
                    }
                };
            } else {
                scan = new AppendOnlyFileStoreScan(manifests, buckets, snapshotManager(), schemaManager(), schema,
                        manifestFiles, options.scanManifestParallelism(), options.fileIndexReadEnabled(),
                        options.deletionVectorsEnabled(), false) {
                    @Override
                    public Iterator<ManifestEntry> readManifestEntries(List<ManifestFileMeta> files, boolean sequential) {
                        return readEntries(files, scanMode, parallelism(), planningIo, this::readManifest);
                    }
                };
            }
            return new SnapshotReaderImpl(scan, schema, options, snapshotManager(), changelogManager(),
                    splitGenerator(), nonPartitionFilterConsumer(), store.pathFactory(), name(),
                    store.newIndexFileHandler(), dvmetaCache);
        }
    }

    static final class PrimaryKey extends PrimaryKeyFileStoreTable {
        private static final long serialVersionUID = 1L;
        private final transient PaimonPlanningFileIO planningIo;

        PrimaryKey(PaimonPlanningFileIO io, Path path, TableSchema schema, CatalogEnvironment environment) {
            super(io, path, schema, environment);
            this.planningIo = io;
        }

        @Override
        public SnapshotReader newSnapshotReader() {
            var store = store();
            var schema = schema();
            var options = coreOptions();
            var buckets = new BucketSelectConverter(store.bucketMode(), options.bucketFunctionType(),
                    schema.logicalRowType(), schema.logicalPartitionType(), schema.logicalBucketKeyType());
            var scan = new KeyValueFileStoreScan(manifestsReader(this), buckets, snapshotManager(), schemaManager(),
                    schema, PrimaryKeyFieldsExtractor.EXTRACTOR, store.manifestFileFactory(),
                    options.scanManifestParallelism(), options.deletionVectorsEnabled(), options.mergeEngine(),
                    options.changelogProducer(), options.fileIndexReadEnabled() && options.deletionVectorsEnabled()) {
                @Override
                public Iterator<ManifestEntry> readManifestEntries(List<ManifestFileMeta> files, boolean sequential) {
                    return readEntries(files, scanMode, parallelism(), planningIo, this::readManifest);
                }
            };
            return new SnapshotReaderImpl(scan, schema, options, snapshotManager(), changelogManager(),
                    splitGenerator(), nonPartitionFilterConsumer(), store.pathFactory(), name(),
                    store.newIndexFileHandler(), dvmetaCache);
        }
    }
}
