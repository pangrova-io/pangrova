package org.pangrova.worker.resource.memory;

import org.pangrova.worker.exception.DoubleReleaseException;
import org.pangrova.worker.exception.InsufficientMemoryException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the full memory pool hierarchy:
 * Root → Node → Query → Task → Operator.
 */
class MemoryPoolHierarchyTest {

    @Test
    void rootPoolReserveAndFree() {
        var root = new RootMemoryPool("root", 1024);
        assertThat(root.getMaxBytes()).isEqualTo(1024);
        assertThat(root.getUsedBytes()).isZero();
        assertThat(root.getFreeBytes()).isEqualTo(1024);

        long reserved = root.tryReserve(256);
        assertThat(reserved).isEqualTo(256);
        assertThat(root.getUsedBytes()).isEqualTo(256);

        root.free(256);
        assertThat(root.getUsedBytes()).isZero();
    }

    @Test
    void tryReserveReturnsPartialWhenInsufficient() {
        var root = new RootMemoryPool("root", 100);
        root.tryReserve(80);
        long reserved = root.tryReserve(50);
        assertThat(reserved).isEqualTo(20);
    }

    @Test
    void tryReserveReturnsZeroWhenFull() {
        var root = new RootMemoryPool("root", 100);
        root.tryReserve(100);
        assertThat(root.tryReserve(1)).isZero();
    }

    @Test
    void tryReserveIgnoresNonPositive() {
        var root = new RootMemoryPool("root", 100);
        assertThat(root.tryReserve(0)).isZero();
        assertThat(root.tryReserve(-5)).isZero();
    }

    @Test
    void freeIgnoresNonPositive() {
        var root = new RootMemoryPool("root", 100);
        root.tryReserve(50);
        root.free(0);
        root.free(-10);
        assertThat(root.getUsedBytes()).isEqualTo(50);
    }

    @Test
    void createNodePoolReservesFromRoot() {
        var root = new RootMemoryPool("root", 1024);
        NodeMemoryPool node = root.createNodePool("node-1", 512);

        assertThat(node.getMaxBytes()).isEqualTo(512);
        assertThat(node.getParent()).isSameAs(root);
        assertThat(root.getUsedBytes()).isEqualTo(512);
    }

    @Test
    void createNodePoolThrowsWhenRootFull() {
        var root = new RootMemoryPool("root", 100);
        root.createNodePool("node-1", 100);

        assertThatThrownBy(() -> root.createNodePool("node-2", 50))
                .isInstanceOf(InsufficientMemoryException.class)
                .hasMessageContaining("root")
                .hasMessageContaining("requested=50");
    }

    @Test
    void fullHierarchyCreation() {
        var root = new RootMemoryPool("root", 4096);
        NodeMemoryPool node = root.createNodePool("node-1", 2048);
        QueryMemoryPool query = node.createQueryPool("query-1", 1024);
        TaskMemoryPool task = query.createTaskPool("task-1", 512);
        OperatorMemoryPool op = task.createOperatorPool("scan-0", 256);

        assertThat(op.getMaxBytes()).isEqualTo(256);
        assertThat(op.getParent()).isSameAs(task);
        assertThat(task.getParent()).isSameAs(query);
        assertThat(query.getParent()).isSameAs(node);
        assertThat(node.getParent()).isSameAs(root);
    }

    @Test
    void childCannotExceedParentQuota() {
        var root = new RootMemoryPool("root", 1024);
        NodeMemoryPool node = root.createNodePool("node-1", 512);

        assertThatThrownBy(() -> node.createQueryPool("q-1", 600))
                .isInstanceOf(InsufficientMemoryException.class);
    }

    @Test
    void operatorReserveAndFreeWithinTask() {
        var root = new RootMemoryPool("root", 4096);
        NodeMemoryPool node = root.createNodePool("node-1", 2048);
        QueryMemoryPool query = node.createQueryPool("q-1", 1024);
        TaskMemoryPool task = query.createTaskPool("t-1", 512);
        OperatorMemoryPool op = task.createOperatorPool("scan-0", 256);

        long reserved = op.tryReserve(100);
        assertThat(reserved).isEqualTo(100);
        assertThat(op.getUsedBytes()).isEqualTo(100);

        op.free(100);
        assertThat(op.getUsedBytes()).isZero();
    }

    @Test
    void constructorRejectsNegativeMaxBytes() {
        assertThatThrownBy(() -> new RootMemoryPool("bad", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    @Test
    void freeMoreThanReserved_throwsDoubleReleaseException() {
        var root = new RootMemoryPool("test-pool", 1024);
        root.tryReserve(100);

        assertThatThrownBy(() -> root.free(200))
                .isInstanceOf(DoubleReleaseException.class)
                .hasMessageContaining("test-pool")
                .hasMessageContaining("200")
                .hasMessageContaining("100");

        // Usage unchanged after failed release
        assertThat(root.getUsedBytes()).isEqualTo(100);
        // Metric counter incremented
        assertThat(root.getDoubleReleaseCount()).isEqualTo(1);
    }

    @Test
    void doubleRelease_detectsSecondFreeOfSameAllocation() {
        var root = new RootMemoryPool("dr-pool", 1024);
        root.tryReserve(256);

        // First free succeeds
        root.free(256);
        assertThat(root.getUsedBytes()).isZero();

        // Second free of same allocation is a double-release
        assertThatThrownBy(() -> root.free(256))
                .isInstanceOf(DoubleReleaseException.class)
                .hasMessageContaining("dr-pool");

        assertThat(root.getDoubleReleaseCount()).isEqualTo(1);
    }

    @Test
    void doubleReleaseException_carriesCorrectContext() {
        var root = new RootMemoryPool("ctx-pool", 512);
        root.tryReserve(50);

        try {
            root.free(100);
        } catch (DoubleReleaseException e) {
            assertThat(e.getPoolName()).isEqualTo("ctx-pool");
            assertThat(e.getReleaseBytes()).isEqualTo(100);
            assertThat(e.getCurrentUsed()).isEqualTo(50);
            return;
        }
        throw new AssertionError("Expected DoubleReleaseException");
    }
}
