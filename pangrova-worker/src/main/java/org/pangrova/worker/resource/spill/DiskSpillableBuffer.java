/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.worker.resource.spill;

import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Disk-backed spillable buffer that stores byte[] batches in memory and spills
 * oldest batches to local disk under memory pressure.
 *
 * <p>Thread-safe: all mutation (add, spill, readBack, close) is serialized
 * via a single lock. Read operations (reclaimableBytes, spillId) use volatile
 * or immutable state and are lock-free.</p>
 */
@ThreadSafe
public final class DiskSpillableBuffer implements SpillableBuffer, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DiskSpillableBuffer.class);

    private final String id;
    private final int priority;
    private final Path spillDirectory;
    private final ReentrantLock lock = new ReentrantLock();

    private final Deque<byte[]> memoryBatches = new ArrayDeque<>();
    private final Deque<Path> spilledFiles = new ArrayDeque<>();
    private final Deque<Path> cleanupFiles = new ArrayDeque<>();
    private volatile long memoryBytes;
    private boolean closed;

    /** Creates a buffer owning unique spill files under the configured directory. */
    public DiskSpillableBuffer(String id, int priority, Path spillDirectory) {
        this.id = Objects.requireNonNull(id, "id");
        this.priority = priority;
        if (priority <= 0) {
            throw new IllegalArgumentException("priority must be positive");
        }
        this.spillDirectory = Objects.requireNonNull(spillDirectory, "spillDirectory");
    }

    /**
     * Appends a batch to the in-memory portion of the buffer.
     */
    public void addBatch(byte[] batch) {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Buffer " + id + " is closed");
            }
            Objects.requireNonNull(batch, "batch");
            long updatedMemoryBytes = Math.addExact(memoryBytes, batch.length);
            memoryBatches.addLast(batch);
            memoryBytes = updatedMemoryBytes;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String spillId() {
        return id;
    }

    @Override
    public int spillPriority() {
        return priority;
    }

    @Override
    public long reclaimableBytes() {
        return memoryBytes;
    }

    @Override
    public long spillToDisk(long targetBytes) {
        lock.lock();
        try {
            if (closed || targetBytes <= 0 || memoryBatches.isEmpty()) {
                return 0;
            }
            ensureSpillDirectory();
            long freed = 0;
            while (!memoryBatches.isEmpty() && freed < targetBytes) {
                byte[] batch = memoryBatches.peekFirst();
                Path spillFile = null;
                try {
                    // Unique owner files avoid collisions and never incorporate caller-supplied ids.
                    spillFile = Files.createTempFile(spillDirectory, "pangrova-", ".spill");
                    Files.write(spillFile, batch);
                } catch (IOException e) {
                    if (spillFile != null) {
                        try {
                            Files.deleteIfExists(spillFile);
                        } catch (IOException cleanupFailure) {
                            cleanupFiles.addLast(spillFile);
                            e.addSuppressed(cleanupFailure);
                        }
                    }
                    throw new UncheckedIOException("Failed to spill buffer " + id, e);
                }
                spilledFiles.addLast(spillFile);
                memoryBatches.removeFirst();
                memoryBytes -= batch.length;
                freed += batch.length;
            }
            LOG.debug("event=spill_to_disk id={} freedBytes={} remainingMemory={} spilledFiles={}",
                    id, freed, memoryBytes, spilledFiles.size());
            return freed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads back all spilled data from disk into memory, restoring original order.
     * Spilled files are deleted after successful read-back.
     */
    public void readBack() {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Buffer " + id + " is closed");
            }
            Deque<byte[]> restored = new ArrayDeque<>();
            long restoredBytes = 0;
            // Read the whole prefix before deleting any files or publishing restored batches.
            for (Path spillFile : spilledFiles) {
                try {
                    byte[] data = Files.readAllBytes(spillFile);
                    restored.addLast(data);
                    restoredBytes = Math.addExact(restoredBytes, data.length);
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to read back " + spillFile, e);
                }
            }
            long totalMemory = Math.addExact(memoryBytes, restoredBytes);
            while (!restored.isEmpty()) {
                memoryBatches.addFirst(restored.removeLast());
            }
            memoryBytes = totalMemory;
            cleanupFiles.addAll(spilledFiles);
            spilledFiles.clear();
            cleanupSpillFiles();
            LOG.debug("event=read_back id={} memoryBytes={} batches={}",
                    id, memoryBytes, memoryBatches.size());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the total number of batches held in memory and on spill files.
     */
    public int batchCount() {
        lock.lock();
        try {
            return memoryBatches.size() + spilledFiles.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
            memoryBatches.clear();
            memoryBytes = 0;
            cleanupFiles.addAll(spilledFiles);
            spilledFiles.clear();
            // A failed cleanup remains owned; subsequent close retries it and reports failures.
            cleanupSpillFiles();
        } finally {
            lock.unlock();
        }
    }

    private void cleanupSpillFiles() {
        IOException failure = null;
        for (Iterator<Path> files = cleanupFiles.iterator(); files.hasNext();) {
            Path spillFile = files.next();
            try {
                Files.deleteIfExists(spillFile);
                files.remove();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw new UncheckedIOException("Failed to clean spill files for " + id, failure);
        }
    }

    private void ensureSpillDirectory() {
        try {
            Files.createDirectories(spillDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create spill directory: " + spillDirectory, e);
        }
    }
}
