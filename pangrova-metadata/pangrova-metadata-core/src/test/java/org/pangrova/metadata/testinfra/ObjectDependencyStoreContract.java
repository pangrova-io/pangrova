package org.pangrova.metadata.testinfra;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.relation.CascadePolicy;
import org.pangrova.metadata.relation.ObjectDependencyService;
import org.pangrova.persistence.catalog.api.dependency.ObjectDependencyStore;
import org.pangrova.metadata.api.relation.ObjectKind;
import org.pangrova.metadata.spi.dependency.MetadataMaintenanceHook;
import org.pangrova.metadata.api.relation.ObjectRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The same ordering and cascade behavior against memory and migrated PostgreSQL stores. */
public interface ObjectDependencyStoreContract {
    ObjectDependencyStore dependencyStore();

    private static ObjectRef object(String name) {
        return new ObjectRef("ordering", "contract", name, ObjectKind.VIEW);
    }

    private void shortcutGraph() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        store.recordDependencies(object("d"), Set.of(object("b")));
        store.recordDependencies(object("c"), Set.of(object("a"), object("d")));
    }

    @Test
    default void shortcutOrdersDependentsBeforeAllTheirParents() {
        shortcutGraph();
        assertThat(dependencyStore().getTransitiveDependentsOf(object("a")))
                .containsExactly(object("c"), object("d"), object("b"));
    }

    @Test
    default void sharedDescendantsAppearOnceBeforeBothBranches() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        store.recordDependencies(object("c"), Set.of(object("a")));
        store.recordDependencies(object("d"), Set.of(object("b"), object("c")));
        var order = store.getTransitiveDependentsOf(object("a"));
        assertThat(order).containsExactlyInAnyOrder(object("b"), object("c"), object("d"));
        assertThat(order.indexOf(object("d"))).isLessThan(order.indexOf(object("b")));
        assertThat(order.indexOf(object("d"))).isLessThan(order.indexOf(object("c")));
    }

    @Test
    default void cyclesTerminateWithoutReturningRootOrDuplicatingReachableObjects() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a"), object("c")));
        store.recordDependencies(object("c"), Set.of(object("b")));
        store.recordDependencies(object("a"), Set.of(object("c")));
        store.recordDependencies(object("d"), Set.of(object("c")));
        assertThat(store.getTransitiveDependentsOf(object("a")))
                .containsExactlyInAnyOrder(object("b"), object("c"), object("d"));
    }

    @Test
    default void failedCascadeRetainsEdgesForEveryUndroppedObject() {
        shortcutGraph();
        var store = dependencyStore();
        var service = new ObjectDependencyService(store);
        List<ObjectRef> dropped = new ArrayList<>();
        var failure = new IllegalStateException("connector drop rejected");
        assertThatThrownBy(() -> service.applyDropPolicy(object("a"), CascadePolicy.CASCADE, object -> {
            if (object.equals(object("d"))) throw failure;
            assertThat(store.getDependentsOf(object)).isEmpty();
            dropped.add(object);
        })).isSameAs(failure);
        assertThat(dropped).containsExactly(object("c"));
        assertThat(store.getParentsOf(object("c"))).isEmpty();
        assertThat(store.getParentsOf(object("d"))).containsExactly(object("b"));
        assertThat(store.getParentsOf(object("b"))).containsExactly(object("a"));
    }

    private static ObjectRef asTable(ObjectRef ref) {
        return new ObjectRef(ref.catalog(), ref.schema(), ref.name(), ObjectKind.TABLE);
    }

    @Test
    default void lookupsUseNaturalNameRatherThanKind() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        assertThat(store.getParentsOf(asTable(object("b")))).containsExactly(object("a"));
        assertThat(store.getDependentsOf(asTable(object("a")))).containsExactly(object("b"));
    }

    @Test
    default void recordingSameNaturalEdgeUpdatesItsKindMetadataWithoutDuplicatingIt() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a"), object("c")));
        store.recordDependencies(asTable(object("b")), Set.of(asTable(object("a"))));
        assertThat(store.getParentsOf(object("b")))
                .containsExactlyInAnyOrder(asTable(object("a")), object("c"));
        assertThat(store.getDependentsOf(object("a"))).containsExactly(asTable(object("b")));
        // Metadata belongs to each edge; updating A->B does not rewrite C->B.
        assertThat(store.getDependentsOf(object("c"))).containsExactly(object("b"));
    }

    @Test
    default void replacementMatchesExistingChildByNaturalName() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        store.replaceDependenciesForChild(asTable(object("b")), Set.of(object("c")));
        assertThat(store.getParentsOf(object("b"))).containsExactly(object("c"));
        assertThat(store.getDependentsOf(object("a"))).isEmpty();
        assertThat(store.getDependentsOf(object("c"))).containsExactly(asTable(object("b")));
    }

    @Test
    default void removalMatchesBothSidesByNaturalNameAndCountsEachEdge() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        store.recordDependencies(object("c"), Set.of(object("b")));
        assertThat(store.removeAllEdgesFor(asTable(object("b")))).isEqualTo(2);
        assertThat(store.getParentsOf(object("b"))).isEmpty();
        assertThat(store.getDependentsOf(object("a"))).isEmpty();
        assertThat(store.getParentsOf(object("c"))).isEmpty();
    }

    @Test
    default void sameNaturalNameCannotCreateASelfEdgeWithDifferentKinds() {
        var store = dependencyStore();
        store.recordDependencies(object("a"), Set.of(asTable(object("a"))));
        assertThat(store.getParentsOf(object("a"))).isEmpty();
        assertThat(store.getDependentsOf(object("a"))).isEmpty();
        store.recordDependencies(object("a"), Set.of(object("b")));
        store.replaceDependenciesForChild(object("a"), Set.of(asTable(object("a"))));
        assertThat(store.getParentsOf(object("a"))).isEmpty();
        assertThat(store.getDependentsOf(object("a"))).isEmpty();
        assertThat(store.getDependentsOf(object("b"))).isEmpty();
    }

    @Test
    default void traversalVisitsOneNaturalObjectDespitePerEdgeKindMetadata() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        store.recordDependencies(object("c"), Set.of(object("a")));
        store.recordDependencies(asTable(object("b")), Set.of(object("c")));
        assertThat(store.getTransitiveDependentsOf(object("a")))
                .extracting(ObjectRef::name).containsExactly("b", "c");
    }

    @Test
    default void traversalExcludesRootNaturalIdentityEvenWhenCycleChangesKind() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        store.recordDependencies(object("c"), Set.of(asTable(object("b"))));
        store.recordDependencies(asTable(object("a")), Set.of(object("c")));
        assertThat(store.getTransitiveDependentsOf(object("a")))
                .containsExactly(object("c"), object("b"));
    }

    @Test
    default void invalidParentCannotPartiallyAddEdges() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        var invalidParents = new java.util.LinkedHashSet<ObjectRef>();
        invalidParents.add(object("c"));
        invalidParents.add(null);
        assertThatThrownBy(() -> store.recordDependencies(object("b"), invalidParents))
                .isInstanceOf(NullPointerException.class);
        assertThat(store.getParentsOf(object("b"))).containsExactly(object("a"));
        assertThat(store.getDependentsOf(object("c"))).isEmpty();
    }

    @Test
    default void invalidParentCannotPartiallyReplaceEdges() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        var invalidParents = new java.util.LinkedHashSet<ObjectRef>();
        invalidParents.add(object("c"));
        invalidParents.add(null);
        assertThatThrownBy(() -> store.replaceDependenciesForChild(object("b"), invalidParents))
                .isInstanceOf(NullPointerException.class);
        assertThat(store.getParentsOf(object("b"))).containsExactly(object("a"));
        assertThat(store.getDependentsOf(object("a"))).containsExactly(object("b"));
        assertThat(store.getDependentsOf(object("c"))).isEmpty();
    }


    private static MetadataMaintenanceHook captureParents(List<Set<ObjectRef>> snapshots) {
        return new MetadataMaintenanceHook() {
            @Override
            public void onDependenciesChanged(ObjectRef child, Set<ObjectRef> parents) {
                snapshots.add(parents);
            }
        };
    }

    @Test
    default void singleDependencyNotifiesTheCompleteParentSet() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        var service = new ObjectDependencyService(store);
        List<Set<ObjectRef>> snapshots = new ArrayList<>();
        service.addMaintenanceHook(captureParents(snapshots));
        service.recordDependency(object("b"), object("c"));
        assertThat(snapshots).containsExactly(Set.of(object("a"), object("c")));
    }

    @Test
    default void incrementalNotificationIncludesExistingParentsAndExcludesSelfEdges() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        var service = new ObjectDependencyService(store);
        List<Set<ObjectRef>> snapshots = new ArrayList<>();
        service.addMaintenanceHook(captureParents(snapshots));
        service.recordDependencies(object("b"), Set.of(object("b"), object("c")));
        assertThat(snapshots).containsExactly(Set.of(object("a"), object("c")));
    }

    @Test
    default void replacementNotifiesTheStoredSetIncludingEmptyReplacement() {
        var service = new ObjectDependencyService(dependencyStore());
        service.recordDependencies(object("b"), Set.of(object("a")));
        List<Set<ObjectRef>> snapshots = new ArrayList<>();
        service.addMaintenanceHook(captureParents(snapshots));
        service.replaceDependencies(object("b"), Set.of(object("b"), object("c")));
        service.replaceDependencies(object("b"), Set.of());
        assertThat(snapshots).containsExactly(Set.of(object("c")), Set.of());
    }

    @Test
    default void dependencySnapshotsAreImmutableAndCannotCorruptOtherListeners() {
        var service = new ObjectDependencyService(dependencyStore());
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onDependenciesChanged(ObjectRef child, Set<ObjectRef> parents) {
                parents.clear();
            }
        });
        List<Set<ObjectRef>> snapshots = new ArrayList<>();
        service.addMaintenanceHook(captureParents(snapshots));
        var input = new java.util.LinkedHashSet<>(Set.of(object("a")));
        service.recordDependencies(object("b"), input);
        assertThat(input).containsExactly(object("a"));
        assertThat(snapshots).containsExactly(Set.of(object("a")));
        assertThatThrownBy(() -> snapshots.get(0).add(object("c")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    default void failedCascadeNotifiesEachSuccessfullyDroppedObjectImmediately() {
        shortcutGraph();
        var store = dependencyStore();
        var service = new ObjectDependencyService(store);
        List<ObjectRef> notifications = new ArrayList<>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef dropped, List<ObjectRef> cascade) {
                assertThat(store.getParentsOf(dropped)).isEmpty();
                assertThat(store.getDependentsOf(dropped)).isEmpty();
                notifications.add(dropped);
            }
        });
        var failure = new IllegalStateException("connector drop rejected");
        assertThatThrownBy(() -> service.applyDropPolicy(object("a"), CascadePolicy.CASCADE, ref -> {
            if (ref.equals(object("d"))) throw failure;
        })).isSameAs(failure);
        assertThat(notifications).containsExactly(object("c"));
        assertThat(store.getParentsOf(object("d"))).containsExactly(object("b"));
    }

    @Test
    default void warnPreservesEdgesUntilTheConnectorDropActuallySucceeds() {
        var store = dependencyStore();
        store.recordDependencies(object("b"), Set.of(object("a")));
        var service = new ObjectDependencyService(store);
        List<ObjectRef> notifications = new ArrayList<>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef dropped, List<ObjectRef> cascade) {
                notifications.add(dropped);
            }
        });
        assertThat(service.applyDropPolicy(object("a"), CascadePolicy.WARN,
                ref -> { throw new AssertionError("WARN must not drop dependents"); }))
                .containsExactly(object("b"));
        // A failed connector drop must not leave an existing object without its edges.
        assertThat(store.getParentsOf(object("b"))).containsExactly(object("a"));
        assertThat(notifications).isEmpty();
        // Only the explicit successful-drop callback can now remove the graph edges.
        assertThat(service.onObjectDropped(object("a"))).isEqualTo(1);
        assertThat(store.getParentsOf(object("b"))).isEmpty();
        assertThat(notifications).containsExactly(object("a"));
    }

    @Test
    default void invalidMutationCannotNotifyListeners() {
        var service = new ObjectDependencyService(dependencyStore());
        service.recordDependency(object("b"), object("a"));
        List<Set<ObjectRef>> notifications = new ArrayList<>();
        service.addMaintenanceHook(captureParents(notifications));
        var invalid = new java.util.LinkedHashSet<ObjectRef>();
        invalid.add(object("c"));
        invalid.add(null);
        assertThatThrownBy(() -> service.replaceDependencies(object("b"), invalid))
                .isInstanceOf(NullPointerException.class);
        assertThat(notifications).isEmpty();
        assertThat(service.getParents(object("b"))).containsExactly(object("a"));
    }


    @Test
    default void mutationResultsAreCompleteDetachedSnapshots() {
        var store = dependencyStore();
        var first = store.recordDependencies(object("b"), Set.of(object("a")));
        var added = store.recordDependencies(object("b"), Set.of(object("c")));
        var replaced = store.replaceDependenciesForChild(object("b"), Set.of(object("b"), object("d")));
        assertThat(store.recordDependencies(object("b"), Set.of())).containsExactly(object("d"));
        assertThat(store.replaceDependenciesForChild(object("b"), Set.of())).isEmpty();
        assertThat(first).containsExactly(object("a"));
        assertThat(added).containsExactlyInAnyOrder(object("a"), object("c"));
        assertThat(replaced).containsExactly(object("d"));
        assertThatThrownBy(() -> added.add(object("d"))).isInstanceOf(UnsupportedOperationException.class);
    }


    @Test
    default void successfulCascadeNotifiesEachObjectOnceThenTheConfirmedParent() {
        shortcutGraph();
        var service = new ObjectDependencyService(dependencyStore());
        List<ObjectRef> notifications = new ArrayList<>();
        List<List<ObjectRef>> contexts = new ArrayList<>();
        service.addMaintenanceHook(new MetadataMaintenanceHook() {
            @Override
            public void onObjectDropped(ObjectRef dropped, List<ObjectRef> cascade) {
                notifications.add(dropped);
                contexts.add(cascade);
            }
        });
        var dropped = service.applyDropPolicy(object("a"), CascadePolicy.CASCADE, ref -> {});
        assertThat(notifications).containsExactly(object("c"), object("d"), object("b"));
        service.onObjectDropped(object("a"), dropped);
        assertThat(notifications).containsExactly(object("c"), object("d"), object("b"), object("a"));
        assertThat(contexts).containsExactly(List.of(), List.of(), List.of(), dropped);
    }

}
