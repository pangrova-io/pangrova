package org.pangrova.connector.iceberg;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.connector.api.clustering.ClusteringMaintenance;
import org.pangrova.connector.api.exception.ClusteringPublicationException;
import org.pangrova.connector.api.enums.ClusteringMode;
import org.pangrova.connector.api.enums.CommitEventType;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.write.IcebergCompactionExecutor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Owns the native sort declaration, snapshot marker and index notification for Iceberg clustering. */
final class IcebergClusteringMaintenance implements ClusteringMaintenance {

    private static final String ABORTED_RUN = "pangrova.recluster.aborted-run-id";

    private final Catalog catalog;
    private final Table table;
    private final IcebergTableHandle handle;
    private final IcebergCommitNotifier notifier;
    private final String tenantId;

    IcebergClusteringMaintenance(Catalog catalog, Table table, IcebergTableHandle handle,
            IcebergCommitNotifier notifier, ConnectorSession session) {
        this.catalog = catalog;
        this.table = table;
        this.handle = handle;
        this.notifier = notifier;
        this.tenantId = session.getProperties().get("tenant_id");
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("Clustering requires a tenant-bound connector session");
        }
    }

    @Override
    public void setOrder(List<String> columns, ClusteringMode mode, Runnable requireAuthority) {
        IcebergClusteringOrder.set(IcebergDeclarationCommit.bindExact(catalog, table, requireAuthority),
                columns, mode);
    }

    @Override
    public Status status() {
        var declaration = IcebergClusteringOrder.read(table);
        Set<String> interleaved = declaration.filter(order -> order.mode() == ClusteringMode.INTERLEAVE)
                .map(this::interleavedFiles).orElse(Set.of());
        boolean linear = declaration.filter(order -> order.mode() == ClusteringMode.LINEAR).isPresent();
        Integer sortId = table.sortOrder().orderId();
        int total = 0;
        int clustered = 0;
        try (var tasks = table.newScan().planFiles()) {
            for (var task : tasks) {
                total++;
                if ((linear && sortId.equals(task.file().sortOrderId()))
                        || interleaved.contains(task.file().path().toString())) {
                    clustered++;
                }
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("Failed to inspect Iceberg clustering files", failure);
        }
        return new Status(declaration.map(IcebergClusteringOrder::columnNames).orElse(List.of()),
                declaration.map(IcebergClusteringOrder::mode).orElse(null), total, clustered);
    }

    private Set<String> interleavedFiles(IcebergClusteringOrder declaration) {
        for (Snapshot snapshot = table.currentSnapshot(); snapshot != null; ) {
            String generation = snapshot.summary().get(IcebergCompactionExecutor.RECLUSTER_GENERATION);
            if (declaration.generation().equals(generation)) {
                return StreamSupport.stream(snapshot.addedDataFiles(table.io()).spliterator(), false)
                        .map(file -> file.path().toString()).collect(Collectors.toUnmodifiableSet());
            }
            Long parent = snapshot.parentId();
            snapshot = parent == null ? null : table.snapshot(parent);
        }
        return Set.of();
    }

    @Override
    public String revision() {
        return revision(table);
    }

    private static String revision(Table table) {
        var metadata = ((HasTableOperations) table).operations().current();
        return Sha256Digests.sha256Hex(metadata.uuid() + "\n" + metadata.metadataFileLocation());
    }

    @Override
    public OptionalLong rewrite(UUID runId, String expectedRevision, StageDispatcher dispatcher,
            Runnable requireAuthority) {
        Table bound = IcebergDeclarationCommit.bindExact(catalog, table, requireAuthority);
        var declaration = requireRewriteRevision(bound, expectedRevision);
        try {
            var staged = dispatcher.stage(expectedRevision, requireAuthority);
            requireAuthority.run();
            if (!IcebergCompactionExecutor.publishRecluster(bound, sortOrder(bound, declaration), runId,
                    declaration.generation(), staged.attemptId(), staged.files())) {
                return OptionalLong.empty();
            }
        } catch (CommitStateUnknownException failure) {
            throw new ClusteringPublicationException("Iceberg clustering publication is unconfirmed", failure);
        }
        try {
            return OptionalLong.of(findPublished(runId).orElseThrow(
                    () -> new IllegalStateException("Iceberg clustering snapshot was not confirmed")));
        } catch (RuntimeException failure) {
            throw new ClusteringPublicationException("Iceberg clustering requires publication confirmation", failure);
        }
    }

    @Override
    public List<String> stage(String expectedRevision, UUID attemptId, Runnable requireActive) {
        var declaration = requireRewriteRevision(table, expectedRevision);
        return IcebergCompactionExecutor.stageRecluster(table, sortOrder(table, declaration), attemptId,
                declaration.mode() == ClusteringMode.LINEAR ? declaration::linear : declaration::interleave,
                requireActive);
    }

    private static IcebergClusteringOrder requireRewriteRevision(Table table, String expectedRevision) {
        if (!expectedRevision.equals(revision(table))) {
            throw new IllegalStateException("Iceberg clustering metadata changed after admission");
        }
        if (IcebergFormatVersions.extract(table) != 2) {
            throw new UnsupportedOperationException("RECLUSTER requires Iceberg format version 2");
        }
        return IcebergClusteringOrder.read(table).orElseThrow(() ->
                new IllegalArgumentException("Configure a clustering order before running RECLUSTER"));
    }

    private static SortOrder sortOrder(Table table, IcebergClusteringOrder declaration) {
        return declaration.mode() == ClusteringMode.LINEAR ? table.sortOrder() : SortOrder.unsorted();
    }

    @Override
    public Recovery reconcile(UUID runId, String expectedRevision, Runnable requireAuthority) {
        var published = findPublished(runId);
        if (published.isPresent()) return new Recovery(published, false);
        String run = runId.toString();
        if (run.equals(table.properties().get(ABORTED_RUN))) {
            return new Recovery(OptionalLong.empty(), true);
        }
        if (expectedRevision == null || !expectedRevision.equals(revision())) {
            return new Recovery(OptionalLong.empty(), false);
        }
        Table bound = IcebergDeclarationCommit.bindExact(catalog, table, requireAuthority);
        if (!expectedRevision.equals(revision(bound))) {
            return new Recovery(OptionalLong.empty(), false);
        }
        // The exact metadata CAS makes the original planned base permanently unwritable.
        bound.updateProperties().set(ABORTED_RUN, run).commit();
        return new Recovery(OptionalLong.empty(), true);
    }

    private OptionalLong findPublished(UUID runId) {
        table.refresh();
        for (Snapshot snapshot : table.snapshots()) {
            if (runId.toString().equals(snapshot.summary().get(IcebergCompactionExecutor.RECLUSTER_RUN))) {
                notifyIndex(snapshot);
                return OptionalLong.of(snapshot.snapshotId());
            }
        }
        return OptionalLong.empty();
    }

    private void notifyIndex(Snapshot snapshot) {
        List<String> added = StreamSupport.stream(snapshot.addedDataFiles(table.io()).spliterator(), false)
                .map(file -> file.path().toString()).sorted().toList();
        List<String> removed = StreamSupport.stream(snapshot.removedDataFiles(table.io()).spliterator(), false)
                .map(file -> file.path().toString()).sorted().toList();
        notifier.notifyCommit(tenantId, handle.schemaName(), handle.tableName(), added, removed,
                Long.toString(snapshot.snapshotId()), CommitEventType.COMPACT,
                Sha256Digests.sha256Hex("recluster:"
                        + snapshot.summary().get(IcebergCompactionExecutor.RECLUSTER_RUN)));
    }

}
