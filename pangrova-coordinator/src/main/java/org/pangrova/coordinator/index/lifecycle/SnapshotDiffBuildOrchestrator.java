package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.build.DataFileInfo;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.connector.index.SnapshotAwareMetadata;
import org.pangrova.spi.connector.index.SnapshotExpiredException;
import org.pangrova.spi.index.IndexDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Orchestrates snapshot-diff based incremental index builds.
 *
 * <p>When a connector provides {@link SnapshotAwareMetadata}, uses the diff
 * between {@code lastIndexedSnapshotId} and the current snapshot to determine
 * which files need indexing. Falls back to full rebuild when the snapshot gap
 * is too large (expired snapshot).</p>
 *
 * <p>If the connector does not support snapshot-diff, callers should use
 * the CommitEvent-based incremental build path instead.</p>
 */
public final class SnapshotDiffBuildOrchestrator {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotDiffBuildOrchestrator.class);

    private final IndexMetadataStore metadataStore;
    private final IndexBuildCoordinator buildCoordinator;
    private final ConnectorAccessorResolver accessorResolver;

    public SnapshotDiffBuildOrchestrator(
            IndexMetadataStore metadataStore,
            IndexBuildCoordinator buildCoordinator,
            ConnectorAccessorResolver accessorResolver) {
        this.metadataStore = metadataStore;
        this.buildCoordinator = buildCoordinator;
        this.accessorResolver = accessorResolver;
    }

    /**
     * Attempts a snapshot-diff incremental build for the given index.
     *
     * @param descriptor        the index to build
     * @param currentSnapshotId the current table snapshot
     * @return true if snapshot-diff build was submitted; false if not available
     */
    public boolean trySnapshotDiffBuild(IndexDescriptor descriptor, String currentSnapshotId) {
        SnapshotAwareMetadata snapshotMetadata = accessorResolver
                .resolveSnapshotAwareMetadata(
                        descriptor.catalogName(), descriptor.schemaName(),
                        descriptor.tableName());
        if (snapshotMetadata == null) {
            return false;
        }
        return executeDiffBuild(descriptor, currentSnapshotId, snapshotMetadata);
    }

    private boolean executeDiffBuild(IndexDescriptor descriptor, String currentSnapshotId,
                                     SnapshotAwareMetadata snapshotMetadata) {
        String lastSnapshot = descriptor.lastIndexedSnapshotId();
        try {
            List<String> addedFiles = snapshotMetadata
                    .listAddedFilesBetween(lastSnapshot, currentSnapshotId);
            List<String> removedFiles = snapshotMetadata
                    .listRemovedFilesBetween(lastSnapshot, currentSnapshotId);

            handleRemovedFiles(descriptor, removedFiles);
            applyAddedFiles(descriptor, addedFiles, currentSnapshotId);

            LOG.info("action=snapshot_diff_build indexId={} added={} removed={} from={} to={}",
                    descriptor.indexId(), addedFiles.size(), removedFiles.size(),
                    lastSnapshot, currentSnapshotId);
            return true;
        } catch (SnapshotExpiredException e) {
            handleSnapshotExpired(descriptor, e);
            return true;
        }
    }

    private void applyAddedFiles(IndexDescriptor descriptor, List<String> addedFiles,
                                 String currentSnapshotId) {
        if (!addedFiles.isEmpty()) {
            submitBuildForAddedAndUpdateSnapshot(descriptor, addedFiles, currentSnapshotId);
        } else {
            metadataStore.updateLastIndexedSnapshotId(descriptor.indexId(), currentSnapshotId);
        }
    }

    private void handleSnapshotExpired(IndexDescriptor descriptor, SnapshotExpiredException e) {
        LOG.warn("action=snapshot_expired indexId={} snapshotId={} stale_reason=SNAPSHOT_GAP",
                descriptor.indexId(), e.snapshotId());
        metadataStore.updateIndexState(descriptor.indexId(), "STALE",
                java.time.Instant.now());
    }

    private void handleRemovedFiles(IndexDescriptor descriptor, List<String> removedFiles) {
        if (!removedFiles.isEmpty()) {
            metadataStore.markOrphanedByDataFiles(descriptor.indexId(), removedFiles);
        }
    }

    private void submitBuildForAdded(IndexDescriptor descriptor, List<String> addedFiles) {
        if (addedFiles.isEmpty()) {
            return;
        }
        List<DataFileInfo> dataFiles = accessorResolver.resolveDataFileInfos(
                descriptor.catalogName(), descriptor.schemaName(),
                descriptor.tableName(), addedFiles);

        FileSystemAccessor fsAccessor = accessorResolver.resolve(descriptor.catalogName());
        ColumnValueReader columnReader = accessorResolver
                .resolveColumnValueReader(descriptor.catalogName());

        buildCoordinator.submitBuildTask(descriptor, dataFiles, fsAccessor, columnReader);
    }

    private void submitBuildForAddedAndUpdateSnapshot(
            IndexDescriptor descriptor, List<String> addedFiles, String snapshotId) {
        List<DataFileInfo> dataFiles = accessorResolver.resolveDataFileInfos(
                descriptor.catalogName(), descriptor.schemaName(),
                descriptor.tableName(), addedFiles);

        FileSystemAccessor fsAccessor = accessorResolver.resolve(descriptor.catalogName());
        ColumnValueReader columnReader = accessorResolver
                .resolveColumnValueReader(descriptor.catalogName());

        Future<?> buildFuture = buildCoordinator.submitBuildTask(
                descriptor, dataFiles, fsAccessor, columnReader);
        // Update snapshotId only after build submission succeeds.
        // The build itself is async, but if submission fails (e.g. capacity full),
        // the old snapshotId is preserved for retry on next cycle.
        metadataStore.updateLastIndexedSnapshotId(descriptor.indexId(), snapshotId);
    }
}
