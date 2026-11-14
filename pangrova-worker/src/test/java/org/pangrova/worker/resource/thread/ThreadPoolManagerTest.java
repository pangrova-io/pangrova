package org.pangrova.worker.resource.thread;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ThreadBudgetTracker}.
 *
 * <p><b>Validates: Requirements 14.7</b></p>
 */
class ThreadPoolManagerTest {

    private ThreadBudgetTracker manager;

    @BeforeEach
    void setUp() {
        manager = new ThreadBudgetTracker(4, 2, 1);
    }

    @Test
    void maxThreadsIsSumOfPools() {
        assertThat(manager.getMaxThreads()).isEqualTo(7);
    }

    @Test
    void initialAllocatedIsZero() {
        assertThat(manager.getAllocatedThreads()).isEqualTo(0);
        assertThat(manager.getAvailableThreads()).isEqualTo(7);
    }

    @Test
    void allocateThreadsReturnsRequestedWhenAvailable() {
        int allocated = manager.allocateThreads(3);
        assertThat(allocated).isEqualTo(3);
        assertThat(manager.getAllocatedThreads()).isEqualTo(3);
        assertThat(manager.getAvailableThreads()).isEqualTo(4);
    }

    @Test
    void allocateThreadsCapsAtAvailable() {
        int allocated = manager.allocateThreads(10);
        assertThat(allocated).isEqualTo(7);
        assertThat(manager.getAllocatedThreads()).isEqualTo(7);
    }

    @Test
    void allocateZeroReturnsZero() {
        assertThat(manager.allocateThreads(0)).isEqualTo(0);
    }

    @Test
    void allocateNegativeReturnsZero() {
        assertThat(manager.allocateThreads(-1)).isEqualTo(0);
    }

    @Test
    void releaseThreadsDecrementsAllocated() {
        manager.allocateThreads(5);
        manager.releaseThreads(3);
        assertThat(manager.getAllocatedThreads()).isEqualTo(2);
    }

    @Test
    void releaseMoreThanAllocatedCapsAtZero() {
        manager.allocateThreads(3);
        manager.releaseThreads(10);
        assertThat(manager.getAllocatedThreads()).isEqualTo(0);
    }

    @Test
    void releaseZeroIsNoOp() {
        manager.allocateThreads(3);
        manager.releaseThreads(0);
        assertThat(manager.getAllocatedThreads()).isEqualTo(3);
    }

    @Test
    void releaseNegativeIsNoOp() {
        manager.allocateThreads(3);
        manager.releaseThreads(-1);
        assertThat(manager.getAllocatedThreads()).isEqualTo(3);
    }

    @Test
    void shutdownReleasesAll() {
        manager.allocateThreads(5);
        manager.shutdown();
        assertThat(manager.getAllocatedThreads()).isEqualTo(0);
        assertThat(manager.getAvailableThreads()).isEqualTo(7);
    }

    @Test
    void allocateAfterFullReturnZero() {
        manager.allocateThreads(7);
        assertThat(manager.allocateThreads(1)).isEqualTo(0);
    }
}
