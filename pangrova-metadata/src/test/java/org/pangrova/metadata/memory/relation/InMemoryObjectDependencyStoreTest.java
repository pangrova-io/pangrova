package org.pangrova.metadata.memory.relation;

import org.pangrova.metadata.api.relation.ObjectDependencyStore;
import org.pangrova.metadata.api.relation.ObjectKind;
import org.pangrova.metadata.api.relation.ObjectRef;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link InMemoryObjectDependencyStore}: basic CRUD, transitive
 * traversal, cycle guard, and idempotent edge recording.
 */
class InMemoryObjectDependencyStoreTest implements org.pangrova.metadata.testinfra.ObjectDependencyStoreContract {

    private InMemoryObjectDependencyStore store;

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
    }

    @Override
    public ObjectDependencyStore dependencyStore() { return store; }

    @Test
    void deepGraphDoesNotDependOnTheJavaCallStack() {
        int depth = 20_000;
        ObjectRef previous = TABLE_USERS;
        for (int index = 0; index < depth; index++) {
            var next = new ObjectRef("deep", "chain", "node_" + index, ObjectKind.VIEW);
            store.recordDependencies(next, Set.of(previous));
            previous = next;
        }
        var order = store.getTransitiveDependentsOf(TABLE_USERS);
        assertThat(order).hasSize(depth);
        assertThat(order.get(0)).isEqualTo(previous);
        assertThat(order.get(depth - 1)).isEqualTo(new ObjectRef("deep", "chain", "node_0", ObjectKind.VIEW));
    }

    @Test
    void recordAndGetParents() {
        store.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS, TABLE_ORDERS));
        List<ObjectRef> parents = store.getParentsOf(VIEW_SUMMARY);
        assertThat(parents).containsExactlyInAnyOrder(TABLE_USERS, TABLE_ORDERS);
    }

    @Test
    void recordAndGetDependents() {
        store.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        store.recordDependencies(MV_DAILY, Set.of(TABLE_USERS));
        List<ObjectRef> dependents = store.getDependentsOf(TABLE_USERS);
        assertThat(dependents).containsExactlyInAnyOrder(VIEW_SUMMARY, MV_DAILY);
    }

    @Test
    void emptyParentsIsNoOp() {
        store.recordDependencies(VIEW_SUMMARY, Set.of());
        assertThat(store.getParentsOf(VIEW_SUMMARY)).isEmpty();
    }

    @Test
    void idempotentRecording() {
        store.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        store.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        assertThat(store.getParentsOf(VIEW_SUMMARY)).hasSize(1);
        assertThat(store.getDependentsOf(TABLE_USERS)).hasSize(1);
    }

    @Test
    void replaceDependenciesForChild() {
        store.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        store.replaceDependenciesForChild(VIEW_SUMMARY, Set.of(TABLE_ORDERS));

        assertThat(store.getParentsOf(VIEW_SUMMARY))
                .containsExactly(TABLE_ORDERS);
        // TABLE_USERS should no longer have VIEW_SUMMARY as dependent
        assertThat(store.getDependentsOf(TABLE_USERS)).isEmpty();
        assertThat(store.getDependentsOf(TABLE_ORDERS)).containsExactly(VIEW_SUMMARY);
    }

    @Test
    void removeAllEdgesFor() {
        store.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS, TABLE_ORDERS));
        store.recordDependencies(MV_DAILY, Set.of(VIEW_SUMMARY));

        int removed = store.removeAllEdgesFor(VIEW_SUMMARY);
        assertThat(removed).isGreaterThan(0);

        // VIEW_SUMMARY is gone from both sides
        assertThat(store.getParentsOf(VIEW_SUMMARY)).isEmpty();
        assertThat(store.getDependentsOf(VIEW_SUMMARY)).isEmpty();
        // TABLE_USERS no longer has VIEW_SUMMARY as dependent
        assertThat(store.getDependentsOf(TABLE_USERS)).isEmpty();
        // MV_DAILY lost its parent (VIEW_SUMMARY)
        assertThat(store.getParentsOf(MV_DAILY)).isEmpty();
    }

    @Test
    void getTransitiveDependents() {
        // TABLE_USERS -> VIEW_SUMMARY -> MV_DAILY
        store.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        store.recordDependencies(MV_DAILY, Set.of(VIEW_SUMMARY));

        List<ObjectRef> transitive = store.getTransitiveDependentsOf(TABLE_USERS);
        // Deepest first: MV_DAILY before VIEW_SUMMARY
        assertThat(transitive).containsExactly(MV_DAILY, VIEW_SUMMARY);
    }

    @Test
    void hasDependents() {
        assertThat(store.hasDependents(TABLE_USERS)).isFalse();
        store.recordDependencies(VIEW_SUMMARY, Set.of(TABLE_USERS));
        assertThat(store.hasDependents(TABLE_USERS)).isTrue();
    }

    @Test
    void selfEdgeIsSkipped() {
        store.recordDependencies(TABLE_USERS, Set.of(TABLE_USERS));
        assertThat(store.getParentsOf(TABLE_USERS)).isEmpty();
        assertThat(store.getDependentsOf(TABLE_USERS)).isEmpty();
    }

    @Test
    void noParentsReturnsEmptyList() {
        assertThat(store.getParentsOf(TABLE_USERS)).isEmpty();
    }

    @Test
    void noDependentsReturnsEmptyList() {
        assertThat(store.getDependentsOf(TABLE_USERS)).isEmpty();
    }

    @Test
    void noTransitiveDependentsReturnsEmptyList() {
        assertThat(store.getTransitiveDependentsOf(TABLE_USERS)).isEmpty();
    }
}
