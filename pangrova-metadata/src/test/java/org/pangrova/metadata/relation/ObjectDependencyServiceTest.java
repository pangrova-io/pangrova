package org.pangrova.metadata.relation;

import org.pangrova.metadata.api.relation.CascadePolicy;
import org.pangrova.metadata.api.relation.DependencyViolationException;
import org.pangrova.metadata.api.relation.MetadataMaintenanceHook;
import org.pangrova.metadata.api.relation.ObjectKind;
import org.pangrova.metadata.api.relation.ObjectRef;
import org.pangrova.metadata.memory.relation.InMemoryObjectDependencyStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ObjectDependencyService}: cascade policy enforcement,
 * hook lifecycle, and convenience delegation to the underlying store.
 */
class ObjectDependencyServiceTest {

    private InMemoryObjectDependencyStore store;
    private ObjectDependencyService service;

    private static final ObjectRef TABLE_USERS =
            new ObjectRef("catalog", "public", "users", ObjectKind.TABLE);
    private static final ObjectRef TABLE_ORDERS =
            new ObjectRef("catalog", "public", "orders", ObjectKind.TABLE);
    private static final ObjectRef VIEW_SUMMARY =
            new ObjectRef("catalog", "analytics", "user_summary", ObjectKind.VIEW);
    private static final ObjectRef MV_DAILY =
            new ObjectRef("catalog", "agg", "daily_sales", ObjectKind.MATERIALIZED_VIEW);

    @BeforeEach
    void setUp() {
        store = new InMemoryObjectDependencyStore();
        service = new ObjectDependencyService(store);
    }

    // ── Basic delegation ──

    @Test
    void recordAndQuery() {
        service.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS, TABLE_ORDERS));
        assertThat(service.getParents(VIEW_SUMMARY))
                .containsExactlyInAnyOrder(TABLE_USERS, TABLE_ORDERS);
        assertThat(service.getDependents(TABLE_USERS))
                .containsExactly(VIEW_SUMMARY);
    }

    @Test
    void replaceAndQuery() {
        service.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        service.replaceDependencies(VIEW_SUMMARY, Set.of(TABLE_ORDERS));
        assertThat(service.getParents(VIEW_SUMMARY))
                .containsExactly(TABLE_ORDERS);
        assertThat(service.getDependents(TABLE_USERS)).isEmpty();
    }

    @Test
    void emptyRecordIsSkipped() {
        service.recordDependencies(VIEW_SUMMARY, Set.of());
        assertThat(service.getParents(VIEW_SUMMARY)).isEmpty();
    }

    // ── Drop policy ──

    @Test
    void restrictBlocksDropWithDependents() {
        service.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        assertThatThrownBy(() ->
                service.applyDropPolicy(TABLE_USERS, CascadePolicy.RESTRICT, ref -> {}))
                .isInstanceOf(DependencyViolationException.class)
                .hasMessageContaining("user_summary");
    }

    @Test
    void restrictAllowsDropWithoutDependents() {
        List<ObjectRef> result = service.applyDropPolicy(
                TABLE_USERS, CascadePolicy.RESTRICT, ref -> {});
        assertThat(result).isEmpty();
    }

    @Test
    void cascadeDropsDependentsInOrder() {
        // TABLE_USERS -> VIEW_SUMMARY -> MV_DAILY
        service.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        service.recordDependencies(MV_DAILY, Set.of(VIEW_SUMMARY));

        List<ObjectRef> dropped = new ArrayList<>();
        List<ObjectRef> result = service.applyDropPolicy(
                TABLE_USERS, CascadePolicy.CASCADE, dropped::add);

        // Deepest first
        assertThat(dropped).containsExactly(MV_DAILY, VIEW_SUMMARY);
        assertThat(result).containsExactly(MV_DAILY, VIEW_SUMMARY);
    }

    @Test
    void warnReportsDependentsAndCleansUpOnlyAfterSuccessfulDrop() {
        service.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        List<ObjectRef> result = service.applyDropPolicy(
                TABLE_USERS, CascadePolicy.WARN, ref -> {});
        assertThat(result).containsExactly(VIEW_SUMMARY);
        assertThat(service.getDependents(TABLE_USERS)).containsExactly(VIEW_SUMMARY);
        service.onObjectDropped(TABLE_USERS);
        assertThat(service.getDependents(TABLE_USERS)).isEmpty();
    }

    @Test
    void onObjectDroppedPurgesEdges() {
        service.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        int removed = service.onObjectDropped(TABLE_USERS);
        assertThat(removed).isGreaterThan(0);
        assertThat(service.getDependents(TABLE_USERS)).isEmpty();
    }

    // ── Maintenance hooks ──

    @Test
    void hookFiredOnRecordDependencies() {
        var recorded = new ArrayList<ObjectRef>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onDependenciesChanged(ObjectRef child, Set<ObjectRef> newParents) {
                recorded.add(child);
            }
        });

        service.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        assertThat(recorded).containsExactly(VIEW_SUMMARY);
    }

    @Test
    void hookFiredOnReplaceDependencies() {
        var recorded = new ArrayList<ObjectRef>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onDependenciesChanged(ObjectRef child, Set<ObjectRef> newParents) {
                recorded.add(child);
            }
        });

        service.replaceDependencies(VIEW_SUMMARY, Set.of(TABLE_ORDERS));
        assertThat(recorded).containsExactly(VIEW_SUMMARY);
    }

    @Test
    void hookFiredOnObjectDropped() {
        var droppedRefs = new ArrayList<ObjectRef>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef ref, List<ObjectRef> cascadedDependents) {
                droppedRefs.add(ref);
            }
        });

        service.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        service.onObjectDropped(TABLE_USERS);
        assertThat(droppedRefs).containsExactly(TABLE_USERS);
    }

    @Test
    void hookFiredOnObjectDroppedWithCascade() {
        var droppedRefs = new ArrayList<ObjectRef>();
        var cascadeLists = new ArrayList<List<ObjectRef>>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef ref, List<ObjectRef> cascadedDependents) {
                droppedRefs.add(ref);
                cascadeLists.add(cascadedDependents);
            }
        });

        service.onObjectDropped(TABLE_USERS, List.of(VIEW_SUMMARY, MV_DAILY));
        assertThat(droppedRefs).containsExactly(TABLE_USERS);
        assertThat(cascadeLists.get(0)).containsExactly(VIEW_SUMMARY, MV_DAILY);
    }

    @Test
    void hookFiredOnSnapshotExpired() {
        var expiredRefs = new ArrayList<ObjectRef>();
        var snapshotIds = new ArrayList<Long>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onSnapshotExpired(ObjectRef ref, long snapshotId) {
                expiredRefs.add(ref);
                snapshotIds.add(snapshotId);
            }
        });

        service.notifySnapshotExpired(TABLE_USERS, 42L);
        assertThat(expiredRefs).containsExactly(TABLE_USERS);
        assertThat(snapshotIds).containsExactly(42L);
    }

    @Test
    void hookExceptionDoesNotPreventOtherHooks() {
        AtomicInteger invocationCount = new AtomicInteger(0);
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef ref, List<ObjectRef> cascadedDependents) {
                invocationCount.incrementAndGet();
                throw new RuntimeException("hook-1 failure");
            }
        });
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef ref, List<ObjectRef> cascadedDependents) {
                invocationCount.incrementAndGet();
            }
        });

        service.onObjectDropped(TABLE_USERS);
        assertThat(invocationCount.get()).isEqualTo(2);
    }

    @Test
    void removeMaintenanceHook() {
        AtomicInteger count = new AtomicInteger(0);
        MetadataMaintenanceHook hook = new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef ref, List<ObjectRef> cascadedDependents) {
                count.incrementAndGet();
            }
        };
        service.addMaintenanceHook(hook);
        service.onObjectDropped(TABLE_USERS);
        assertThat(count.get()).isEqualTo(1);

        boolean removed = service.removeMaintenanceHook(hook);
        assertThat(removed).isTrue();

        service.onObjectDropped(TABLE_ORDERS);
        assertThat(count.get()).isEqualTo(1); // not incremented
    }

    @Test
    void hookNotFiredOnEmptyRecord() {
        AtomicInteger count = new AtomicInteger(0);
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onDependenciesChanged(ObjectRef child, Set<ObjectRef> newParents) {
                count.incrementAndGet();
            }
        });
        service.recordDependencies(VIEW_SUMMARY, Set.of());
        assertThat(count.get()).isEqualTo(0);
    }

    @Test
    void dropNotificationCannotMutateCallerDataOrOtherListeners() {
        var input = new ArrayList<>(List.of(VIEW_SUMMARY));
        List<List<ObjectRef>> snapshots = new ArrayList<>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef ref, List<ObjectRef> cascade) { cascade.clear(); }
        });
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef ref, List<ObjectRef> cascade) { snapshots.add(cascade); }
        });
        service.onObjectDropped(TABLE_USERS, input);
        assertThat(input).containsExactly(VIEW_SUMMARY);
        assertThat(snapshots).containsExactly(List.of(VIEW_SUMMARY));
    }

    @Test
    void invalidDropNotificationDataCannotDeleteGraphEdges() {
        service.recordDependency(VIEW_SUMMARY, TABLE_USERS);
        assertThatThrownBy(() -> service.onObjectDropped(TABLE_USERS, null))
                .isInstanceOf(NullPointerException.class);
        assertThat(store.getParentsOf(VIEW_SUMMARY)).containsExactly(TABLE_USERS);
    }

}
