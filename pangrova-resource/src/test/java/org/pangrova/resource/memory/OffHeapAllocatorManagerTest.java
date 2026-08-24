package org.pangrova.resource.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link OffHeapAllocatorManager}.
 * Uses real Arrow allocators — no mocks.
 */
class OffHeapAllocatorManagerTest {

    private OffHeapAllocatorManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close();
        }
    }

    @Test
    void createChildAllocatorWithBoundedLimit() {
        manager = new OffHeapAllocatorManager(1024 * 1024);
        BufferAllocator child = manager.getOrCreateChild("test-child", 512 * 1024);

        assertThat(child).isNotNull();
        assertThat(child.getLimit()).isEqualTo(512 * 1024);
    }

    @Test
    void getOrCreateChildReturnsSameInstanceByName() {
        manager = new OffHeapAllocatorManager(1024 * 1024);
        BufferAllocator first = manager.getOrCreateChild("transport-server", 256 * 1024);
        BufferAllocator second = manager.getOrCreateChild("transport-server", 256 * 1024);

        assertThat(first).isSameAs(second);
    }

    @Test
    void getAllocatedBytesReflectsActualUsage() {
        manager = new OffHeapAllocatorManager(10 * 1024 * 1024);
        BufferAllocator child = manager.getOrCreateChild("vectors", 5 * 1024 * 1024);

        assertThat(manager.getAllocatedBytes()).isZero();

        try (IntVector vector = new IntVector("test", child)) {
            vector.allocateNew(100);
            assertThat(manager.getAllocatedBytes()).isGreaterThan(0);
        }
    }

    @Test
    void closeReleasesAllResources() {
        manager = new OffHeapAllocatorManager(1024 * 1024);
        manager.getOrCreateChild("child-a", 256 * 1024);
        manager.getOrCreateChild("child-b", 256 * 1024);

        manager.close();

        // After close, getOrCreateChild should throw
        assertThatThrownBy(() -> manager.getOrCreateChild("child-c", 128))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");

        // Prevent double-close in tearDown
        manager = null;
    }

    @Test
    void closeIsIdempotent() {
        manager = new OffHeapAllocatorManager(1024 * 1024);
        manager.getOrCreateChild("child", 512 * 1024);

        manager.close();
        // Second close should not throw
        manager.close();

        manager = null;
    }

    @Test
    void peakBytesTracksHighWaterMark() {
        manager = new OffHeapAllocatorManager(10 * 1024 * 1024);
        BufferAllocator child = manager.getOrCreateChild("peak-test", 5 * 1024 * 1024);

        try (IntVector vector = new IntVector("peak", child)) {
            vector.allocateNew(1000);
            long peakDuringAlloc = manager.getPeakBytes();
            assertThat(peakDuringAlloc).isGreaterThan(0);
        }

        // After vector close, peak should still reflect the high-water mark
        assertThat(manager.getPeakBytes()).isGreaterThan(0);
    }

    @Test
    void rootReturnsRootAllocator() {
        manager = new OffHeapAllocatorManager(1024 * 1024);
        assertThat(manager.root()).isNotNull();
        assertThat(manager.root().getLimit()).isEqualTo(1024 * 1024);
    }

    @Test
    void constructorRejectsNonPositiveMaxBytes() {
        assertThatThrownBy(() -> new OffHeapAllocatorManager(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OffHeapAllocatorManager(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multipleChildrenWithDifferentLimits() {
        manager = new OffHeapAllocatorManager(10 * 1024 * 1024);
        BufferAllocator server = manager.getOrCreateChild("transport-server", 4 * 1024 * 1024);
        BufferAllocator client = manager.getOrCreateChild("transport-client", 2 * 1024 * 1024);
        BufferAllocator writer = manager.getOrCreateChild("result-writer", 1024 * 1024);

        assertThat(server.getLimit()).isEqualTo(4 * 1024 * 1024);
        assertThat(client.getLimit()).isEqualTo(2 * 1024 * 1024);
        assertThat(writer.getLimit()).isEqualTo(1024 * 1024);
    }
}
