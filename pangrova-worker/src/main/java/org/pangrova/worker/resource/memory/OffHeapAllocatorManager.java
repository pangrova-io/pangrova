package org.pangrova.worker.resource.memory;

import org.pangrova.common.concurrency.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Centralized Arrow BufferAllocator hierarchy manager.
 * Creates a single root allocator and provides bounded child allocators
 * for different components (TransportServer, TransportClient, ResultWriter, etc.).
 *
 * <p>Concurrency strategy: ConcurrentHashMap for child registry,
 * AtomicBoolean for idempotent close. All public methods are safe
 * for concurrent access.</p>
 */
@ThreadSafe
public final class OffHeapAllocatorManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OffHeapAllocatorManager.class);

    private final BufferAllocator root;
    private final ConcurrentHashMap<String, BufferAllocator> children = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new manager with a root allocator bounded to maxBytes.
     *
     * @param maxBytes maximum bytes the root allocator can manage
     * @throws IllegalArgumentException if maxBytes is not positive
     */
    public OffHeapAllocatorManager(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got: " + maxBytes);
        }
        this.root = new RootAllocator(maxBytes);
        LOG.info("OffHeapAllocatorManager created, limit={}", maxBytes);
    }

    /**
     * Returns an existing child allocator by name, or creates a new one
     * with the given byte limit.
     *
     * <p>If a child with the given name already exists but was created with
     * a different limit, a warning is logged. The existing allocator is returned
     * unchanged (Arrow allocators cannot be resized after creation).</p>
     *
     * @param name      unique name for the child allocator
     * @param limitBytes maximum bytes this child can allocate
     * @return the child BufferAllocator
     * @throws IllegalStateException if this manager is already closed
     */
    public BufferAllocator getOrCreateChild(String name, long limitBytes) {
        ensureOpen();
        return children.compute(name, (k, existing) -> {
            if (existing != null) {
                warnIfLimitMismatch(k, existing.getLimit(), limitBytes);
                return existing;
            }
            return createChildAllocator(k, limitBytes);
        });
    }

    private void warnIfLimitMismatch(String name, long existingLimit, long requestedLimit) {
        if (existingLimit != requestedLimit) {
            LOG.warn("Child allocator name={} already exists with limit={}, "
                    + "requested limit={} ignored", name, existingLimit, requestedLimit);
        }
    }

    private BufferAllocator createChildAllocator(String name, long limitBytes) {
        LOG.info("Creating child allocator name={}, limit={}", name, limitBytes);
        try {
            return root.newChildAllocator(name, 0, limitBytes);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create child allocator name=" + name
                            + " limitBytes=" + limitBytes
                            + " rootAllocated=" + root.getAllocatedMemory()
                            + " rootLimit=" + root.getLimit()
                            + ": " + e.getMessage(), e);
        }
    }

    /**
     * Returns the root allocator for read-only metrics access.
     *
     * @return the root BufferAllocator
     */
    public BufferAllocator root() {
        return root;
    }

    /**
     * Returns total allocated bytes across all children.
     */
    public long getAllocatedBytes() {
        return root.getAllocatedMemory();
    }

    /**
     * Returns peak allocated bytes since creation.
     */
    public long getPeakBytes() {
        return root.getPeakMemoryAllocation();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeAllChildren();
        root.close();
        LOG.info("OffHeapAllocatorManager closed");
    }

    private void closeAllChildren() {
        children.forEach((name, child) -> {
            try {
                child.close();
            } catch (Exception e) {
                LOG.warn("Failed to close child allocator name={}: {}", name, e.getMessage());
            }
        });
        children.clear();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("OffHeapAllocatorManager is closed");
        }
    }
}
