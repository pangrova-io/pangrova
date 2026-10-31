package org.pangrova.connector.lance;

import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.spi.metadata.NativeDatasetControlPlane;
import org.pangrova.connector.spi.metadata.NativeDatasetScanPage;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

/** Retains at most 256 descriptors; network enumeration never runs under a Java monitor. */
final class LancePartitionSplitSource implements ConnectorSplitSource {
    private final LanceTableHandle table;
    private final ConnectorSession session;
    private final org.pangrova.connector.spi.metadata.AcceptedSourceSnapshot snapshot;
    private final Optional<NativeDatasetControlPlane> control;
    private final AtomicBoolean fetching = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean finished;
    private OptionalLong after = OptionalLong.empty();
    private NativeDatasetScanPage identity;
    private long enumerated;

    LancePartitionSplitSource(LanceTableHandle table, ConnectorSession session,
            Optional<NativeDatasetControlPlane> control,
            org.pangrova.connector.spi.metadata.AcceptedSourceSnapshot snapshot) {
        this.table = table;
        this.snapshot = snapshot;
        if ((table.version() == 0) != (snapshot == null)) {
            throw new IllegalArgumentException("Scan membership is required");
        }
        this.session = session;
        this.control = control;
        finished = table.version() == 0;
        if (!finished && control.isEmpty()) {
            throw new IllegalStateException("Lance scan requires Native dataset control");
        }
    }

    @Override public List<ConnectorSplit> getNextBatch(int maxSize) {
        if (maxSize <= 0) throw new IllegalArgumentException("maxSize must be positive");
        if (finished || closed.get()) return List.of();
        if (!fetching.compareAndSet(false, true)) throw new IllegalStateException("Concurrent split enumeration");
        try {
            var page = control.orElseThrow().scanPartitions(session, snapshot, after, Math.min(maxSize, 256));
            validate(page);
            if (closed.get()) return List.of();
            after = page.nextAfter();
            finished = after.isEmpty();
            return page.partitions().stream().<ConnectorSplit>map(partition -> new LanceDataSplit(
                    table, partition, page.serializedCatalogBinding(), page.manifestSha256(), snapshot)).toList();
        } finally {
            fetching.set(false);
        }
    }

    private void validate(NativeDatasetScanPage page) {
        if (!MessageDigest.isEqual(page.manifestSha256(), snapshot.manifestSha256())
                || page.version() != table.version()
                || !MessageDigest.isEqual(page.schemaFingerprint(), table.schemaFence())) {
            throw new IllegalArgumentException("Scan page differs from the pinned table version/schema");
        }
        if (identity == null) identity = page;
        if (identity.totalPartitions() != page.totalPartitions()
                || !MessageDigest.isEqual(identity.manifestSha256(), page.manifestSha256())
                || !MessageDigest.isEqual(identity.serializedCatalogBinding(), page.serializedCatalogBinding())) {
            throw new IllegalArgumentException("Manifest identity changed between scan pages");
        }
        validateCursor(page);
        enumerated = Math.addExact(enumerated, page.partitions().size());
        requireCoverage(page);
    }

    private void validateCursor(NativeDatasetScanPage page) {
        long previous = after.orElse(-1);
        for (var partition : page.partitions()) {
            if (partition.id() <= previous) throw new IllegalArgumentException("Scan partition cursor did not advance");
            previous = partition.id();
        }
    }

    private void requireCoverage(NativeDatasetScanPage page) {
        if (enumerated > page.totalPartitions()
                || (page.nextAfter().isEmpty() && enumerated != page.totalPartitions())) {
            throw new IllegalArgumentException("Scan pages omit or duplicate manifest partitions");
        }
    }

    @Override public boolean isFinished() { return finished || closed.get(); }
    @Override public void close() { closed.set(true); }
}
