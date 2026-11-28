package org.pangrova.worker.buffer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spills shuffle partition data to local disk when memory pressure is high.
 *
 * <p>Thread-safe: concurrent spill/read from multiple partitions via
 * {@link ConcurrentHashMap} for partition → sequence tracking.
 *
 * <p>Implements {@link AutoCloseable} — task completion must call {@link #close()}
 * to clean up spill files.
 *
 * <p>Placement: shuffle/ — shuffle data management.
 */
@ThreadSafe
public final class ShuffleDiskSpiller implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleDiskSpiller.class);

    private final Path spillBaseDir;
    private final long maxSpillBytes;
    private final ConcurrentHashMap<Integer, AtomicInteger> partitionSequences =
            new ConcurrentHashMap<>();
    private final AtomicLong spillBytes = new AtomicLong(0);
    private final AtomicInteger spillFiles = new AtomicInteger(0);
    private volatile boolean closed = false;

    /**
     * Creates a spiller with unlimited disk budget.
     *
     * @param spillBaseDir base directory for spill files (created if absent)
     */
    public ShuffleDiskSpiller(Path spillBaseDir) {
        this(spillBaseDir, 0);
    }

    /**
     * @param spillBaseDir  base directory for spill files (created if absent)
     * @param maxSpillBytes maximum total bytes allowed to spill (0 = unlimited)
     */
    public ShuffleDiskSpiller(Path spillBaseDir, long maxSpillBytes) {
        this.spillBaseDir = spillBaseDir;
        this.maxSpillBytes = maxSpillBytes;
        try {
            Files.createDirectories(spillBaseDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create spill directory: " + spillBaseDir, e);
        }
    }

    /**
     * Spill partition data to disk.
     *
     * @return bytes written
     */
    public long spill(int partitionId, byte[] data) {
        reserveSpillBudget(data.length);
        AtomicInteger seq = partitionSequences.computeIfAbsent(
                partitionId, k -> new AtomicInteger(0));
        int seqId = seq.getAndIncrement();
        Path partDir = spillBaseDir.resolve(String.valueOf(partitionId));
        try {
            Files.createDirectories(partDir);
            Path spillFile = partDir.resolve(seqId + ".dat");
            Files.write(spillFile, data);
            spillFiles.incrementAndGet();
            LOG.debug("event=shuffle_spill partitionId={} seqId={} bytes={}",
                    partitionId, seqId, data.length);
            return data.length;
        } catch (IOException e) {
            // Roll back the reserved bytes on write failure to keep the budget accurate.
            spillBytes.addAndGet(-data.length);
            throw new UncheckedIOException(
                    "Spill failed: partitionId=" + partitionId + " seqId=" + seqId, e);
        }
    }

    /**
     * Atomically reserves spill budget using CAS loop.
     * Prevents concurrent spills from exceeding the configured maximum.
     */
    private void reserveSpillBudget(int requestedBytes) {
        if (maxSpillBytes <= 0) {
            spillBytes.addAndGet(requestedBytes);
            return;
        }
        while (true) {
            long current = spillBytes.get();
            if (current + requestedBytes > maxSpillBytes) {
                throw new IllegalStateException("Spill budget exceeded: current=" + current
                        + " requested=" + requestedBytes + " maxSpillBytes=" + maxSpillBytes);
            }
            if (spillBytes.compareAndSet(current, current + requestedBytes)) {
                return;
            }
        }
    }

    /**
     * Read spilled data for a partition.
     *
     * @return data bytes, or null if not spilled
     */
    public byte[] read(int partitionId, int sequenceId) {
        Path spillFile = spillBaseDir
                .resolve(String.valueOf(partitionId))
                .resolve(sequenceId + ".dat");
        if (!Files.exists(spillFile)) {
            return null;
        }
        try {
            return Files.readAllBytes(spillFile);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Spill read failed: partitionId=" + partitionId + " seqId=" + sequenceId, e);
        }
    }

    /** Total bytes spilled to disk. */
    public long totalSpillBytes() {
        return spillBytes.get();
    }

    /** Total spill files written. */
    public int totalSpillFiles() {
        return spillFiles.get();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try (Stream<Path> walk = Files.walk(spillBaseDir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            LOG.warn("event=spill_cleanup_failed path={} error={}",
                                    p, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            LOG.warn("event=spill_cleanup_walk_failed dir={} error={}",
                    spillBaseDir, e.getMessage());
        }
        LOG.info("event=spill_cleanup_complete dir={} totalBytes={} totalFiles={}",
                spillBaseDir, spillBytes.get(), spillFiles.get());
    }
}
