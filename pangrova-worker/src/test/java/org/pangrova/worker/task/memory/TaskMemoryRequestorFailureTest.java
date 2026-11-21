package org.pangrova.worker.task.memory;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.execution.PlanFragment;
import org.pangrova.protocol.execution.PlanNodeId;
import org.pangrova.execution.api.identity.QueryId;
import org.pangrova.protocol.execution.TaskDescriptor;
import org.pangrova.protocol.execution.TaskResourceLimit;
import org.pangrova.worker.resource.ResourceManager;
import org.pangrova.worker.resource.memory.CriticalPressureHandler;
import org.pangrova.worker.resource.memory.LargestQueryKiller;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.resource.memory.RootMemoryPool;
import org.pangrova.worker.resource.spill.SpillManager;
import org.pangrova.worker.resource.spill.SpillableBuffer;
import org.pangrova.worker.resource.thread.ThreadBudgetTracker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies rollback of descriptor-only credit before its task account takes ownership. */
class TaskMemoryRequestorFailureTest {

    @Test
    void spillFailureRollsBackPartialCreditAndNodeReservation() {
        var fixture = new AllocationFixture(new MemoryArbitrator(100, 0));
        var cause = new IOException("spill volume unavailable");
        var failure = new UncheckedIOException("spill failed", cause);
        fixture.failDuringSpill(() -> {
            fixture.assertPartialAllocation();
            throw failure;
        });

        assertThatThrownBy(() -> fixture.requestor.requestMemoryForTask(fixture.account))
                .isSameAs(failure)
                .hasCause(cause)
                .hasNoSuppressedExceptions();

        fixture.assertRolledBack();
    }

    @Test
    void criticalKillFailureRollsBackPartialCreditAndNodeReservation() {
        var fixture = new AllocationFixture(new MemoryArbitrator(100, 0));
        var victimNode = new RootMemoryPool("victim-root", 32).createNodePool("victim-node", 32);
        var victim = victimNode.createQueryPool("victim", 16);
        assertThat(victim.tryReserve(8)).isEqualTo(8);
        var cause = new IllegalStateException("query cancellation failed");
        var failure = new IllegalStateException("critical pressure callback failed", cause);
        var handler = new CriticalPressureHandler(
                new LargestQueryKiller(), victimNode::getActiveQueryPools, pool -> {
                    assertThat(pool).isSameAs(victim);
                    fixture.assertPartialAllocation();
                    throw failure;
                });
        fixture.configure(new SpillManager(), handler);

        assertThatThrownBy(() -> fixture.requestor.requestMemoryForTask(fixture.account))
                .isSameAs(failure)
                .hasCause(cause)
                .hasNoSuppressedExceptions();

        fixture.assertRolledBack();
        assertThat(victim.getUsedBytes()).isEqualTo(8);
    }

    @Test
    void spillErrorRollsBackPartialCreditAndNodeReservation() {
        var fixture = new AllocationFixture(new MemoryArbitrator(100, 0));
        var failure = new AssertionError("spill owner failed");
        fixture.failDuringSpill(() -> {
            fixture.assertPartialAllocation();
            throw failure;
        });

        assertThatThrownBy(() -> fixture.requestor.requestMemoryForTask(fixture.account))
                .isSameAs(failure)
                .hasNoSuppressedExceptions();

        fixture.assertRolledBack();
    }

    @Test
    void rollbackFailureIsSuppressedWithoutSkippingNodeRelease() {
        var cleanupFailure = new IllegalStateException("release acknowledgement failed");
        var fixture = new AllocationFixture(new FailingReleaseArbitrator(cleanupFailure));
        var failure = new UncheckedIOException("spill failed", new IOException("disk unavailable"));
        fixture.failDuringSpill(() -> {
            fixture.assertPartialAllocation();
            throw failure;
        });

        assertThatThrownBy(() -> fixture.requestor.requestMemoryForTask(fixture.account))
                .isSameAs(failure);
        assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);

        fixture.assertRolledBack();
    }

    private static final class AllocationFixture {
        private final MemoryArbitrator arbitrator;
        private final RootMemoryPool root = new RootMemoryPool("root", 256);
        private final NodeMemoryPool node = root.createNodePool("node", 256);
        private final TaskMemoryAccount account;
        private final TaskMemoryRequestor requestor;

        private AllocationFixture(MemoryArbitrator arbitrator) {
            this.arbitrator = arbitrator;
            // Preserve both an unrelated query and a sibling task in this query.
            assertThat(arbitrator.requestMemory("other-query", 48)).isEqualTo(48);
            assertThat(arbitrator.requestMemory("request-query", 12)).isEqualTo(12);
            assertThat(node.tryReserve(16)).isEqualTo(16);
            var descriptor = new TaskDescriptor(new QueryId("request-query"), new TestPlanFragment(),
                    List.of(), Map.of(), 1, 0, "COMPUTE", new TaskResourceLimit(80, 1, 0, ""));
            account = new TaskMemoryAccount("failing-task", "request-query", null, descriptor);
            requestor = new TaskMemoryRequestor(arbitrator, 0.5);
            requestor.setNodeMemoryPool(node);
        }

        private void configure(SpillManager spillManager, CriticalPressureHandler handler) {
            requestor.setResourceManager(new ResourceManager(arbitrator,
                    new ThreadBudgetTracker(1, 0, 0), spillManager, handler));
        }

        private void failDuringSpill(Runnable action) {
            var spillManager = new SpillManager();
            spillManager.registerCandidate(new FailingSpillBuffer(action).toSpillCandidate());
            configure(spillManager, null);
        }

        private void assertPartialAllocation() {
            assertThat(arbitrator.getUsedMemory()).isEqualTo(100);
            assertThat(arbitrator.queryAllocationsSnapshot())
                    .containsExactlyInAnyOrderEntriesOf(Map.of("other-query", 48L, "request-query", 52L));
            assertThat(node.getUsedBytes()).isEqualTo(96);
            assertThat(account.allocatedMemory()).isZero();
            assertThat(account.allocatedBufferMemory()).isZero();
        }

        private void assertRolledBack() {
            assertBudgetsAtBaseline();
            // Subsequent lifecycle cleanup must not release a sibling task's existing credit.
            requestor.releaseTaskMemory(account);
            requestor.releaseBufferMemory(account);
            assertBudgetsAtBaseline();
        }

        private void assertBudgetsAtBaseline() {
            assertThat(node.getUsedBytes()).isEqualTo(16);
            assertThat(node.getFreeBytes()).isEqualTo(240);
            assertThat(node.getDoubleReleaseCount()).isZero();
            assertThat(arbitrator.getUsedMemory()).isEqualTo(60);
            assertThat(arbitrator.getAvailableMemory()).isEqualTo(40);
            assertThat(arbitrator.queryAllocationsSnapshot())
                    .containsExactlyInAnyOrderEntriesOf(Map.of("other-query", 48L, "request-query", 12L));
            assertThat(root.getUsedBytes()).isEqualTo(256);
            assertThat(account.allocatedMemory()).isZero();
            assertThat(account.allocatedBufferMemory()).isZero();
        }
    }

    private static final class FailingSpillBuffer implements SpillableBuffer {
        private final Runnable action;

        private FailingSpillBuffer(Runnable action) {
            this.action = action;
        }

        @Override
        public String spillId() { return "failing-spill-owner"; }

        @Override
        public int spillPriority() { return 1; }

        @Override
        public long reclaimableBytes() { return 40; }

        @Override
        public long spillToDisk(long targetBytes) {
            assertThat(targetBytes).isEqualTo(40);
            action.run();
            throw new AssertionError("The failure callback must throw");
        }
    }

    /** Real accounting, followed by an injected release-reporting failure. */
    private static final class FailingReleaseArbitrator extends MemoryArbitrator {
        private final RuntimeException failure;

        private FailingReleaseArbitrator(RuntimeException failure) {
            super(100, 0);
            this.failure = failure;
        }

        @Override
        public void release(String queryId, long bytes) {
            super.release(queryId, bytes);
            throw failure;
        }
    }

    private static final class TestPlanFragment implements PlanFragment {
        @Override
        public PlanNodeId getId() { return new PlanNodeId("test-node"); }

        @Override
        public List<PlanNodeId> getChildFragmentIds() { return List.of(); }
    }
}
