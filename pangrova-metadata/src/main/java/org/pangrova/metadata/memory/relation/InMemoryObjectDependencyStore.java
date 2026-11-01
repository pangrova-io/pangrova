package org.pangrova.metadata.memory.relation;

import org.pangrova.metadata.api.relation.ObjectDependencyStore;
import org.pangrova.metadata.api.relation.ObjectRef;

import org.pangrova.metadata.relation.ObjectDependencyKey;
import org.pangrova.metadata.relation.ObjectDependencyOrder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory {@link ObjectDependencyStore}.
 *
 * <p>A process-local implementation for explicit component consumers and
 * tests. Current embedded SQL startup does not register this graph.
 *
 * <p>The internal layout is a forward map (child -> parents) and a reverse
 * map (parent -> children). Both maps are kept in sync under a single
 * read-write lock; reads parallelize, writes serialize. Keys use the natural
 * name; adjacency values retain the kind description written on each edge.
 */
public final class InMemoryObjectDependencyStore implements ObjectDependencyStore {

    private final Map<ObjectDependencyKey, Map<ObjectDependencyKey, ObjectRef>> parentsByChild =
            new LinkedHashMap<>();
    private final Map<ObjectDependencyKey, Map<ObjectDependencyKey, ObjectRef>> childrenByParent =
            new LinkedHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public Set<ObjectRef> recordDependencies(ObjectRef child, Set<ObjectRef> parents) {
        Objects.requireNonNull(child, "child must not be null");
        Objects.requireNonNull(parents, "parents must not be null");
        Set<ObjectRef> parentSnapshot = Set.copyOf(parents);
        lock.writeLock().lock();
        try {
            recordDependenciesUnsafe(child, parentSnapshot);
            return Set.copyOf(getParentsOf(child));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Set<ObjectRef> replaceDependenciesForChild(ObjectRef child, Set<ObjectRef> parents) {
        Objects.requireNonNull(child, "child must not be null");
        Objects.requireNonNull(parents, "parents must not be null");
        Set<ObjectRef> parentSnapshot = Set.copyOf(parents);
        ObjectDependencyKey childKey = ObjectDependencyKey.of(child);
        lock.writeLock().lock();
        try {
            var existing = parentsByChild.remove(childKey);
            removeFromReverseMap(existing, childKey, childrenByParent);
            recordDependenciesUnsafe(child, parentSnapshot);
            return Set.copyOf(getParentsOf(child));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int removeAllEdgesFor(ObjectRef obj) {
        Objects.requireNonNull(obj, "obj must not be null");
        ObjectDependencyKey key = ObjectDependencyKey.of(obj);
        lock.writeLock().lock();
        try {
            // Edges where obj is the child:
            var parents = parentsByChild.remove(key);
            int removed = removeFromReverseMap(parents, key, childrenByParent);
            // Edges where obj is the parent:
            var children = childrenByParent.remove(key);
            removed += removeFromReverseMap(children, key, parentsByChild);
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Removes {@code target} from each adjacency map, cleaning up empty entries. */
    private static int removeFromReverseMap(Map<ObjectDependencyKey, ObjectRef> keys,
                                            ObjectDependencyKey target,
                                            Map<ObjectDependencyKey, Map<ObjectDependencyKey, ObjectRef>> reverseMap) {
        if (keys == null) {
            return 0;
        }
        int removed = 0;
        for (ObjectDependencyKey key : keys.keySet()) {
            var entries = reverseMap.get(key);
            if (entries != null && entries.remove(target) != null) {
                removed++;
                if (entries.isEmpty()) {
                    reverseMap.remove(key);
                }
            }
        }
        return removed;
    }

    @Override
    public List<ObjectRef> getParentsOf(ObjectRef child) {
        Objects.requireNonNull(child, "child must not be null");
        lock.readLock().lock();
        try {
            var parents = parentsByChild.get(ObjectDependencyKey.of(child));
            return parents == null ? List.of() : List.copyOf(parents.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<ObjectRef> getDependentsOf(ObjectRef parent) {
        Objects.requireNonNull(parent, "parent must not be null");
        lock.readLock().lock();
        try {
            var children = childrenByParent.get(ObjectDependencyKey.of(parent));
            return children == null ? List.of() : List.copyOf(children.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<ObjectRef> getTransitiveDependentsOf(ObjectRef parent) {
        Objects.requireNonNull(parent, "parent must not be null");
        lock.readLock().lock();
        try {
            return ObjectDependencyOrder.dependentsFirst(parent, this::getDependentsOf);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void recordDependenciesUnsafe(ObjectRef child, Set<ObjectRef> parents) {
        ObjectDependencyKey childKey = ObjectDependencyKey.of(child);
        for (ObjectRef parent : parents) {
            ObjectDependencyKey parentKey = ObjectDependencyKey.of(parent);
            if (parentKey.equals(childKey)) {
                continue;
            }
            // A repeated natural edge refreshes both descriptions, matching JDBC upsert.
            parentsByChild.computeIfAbsent(childKey, k -> new LinkedHashMap<>()).put(parentKey, parent);
            childrenByParent.computeIfAbsent(parentKey, k -> new LinkedHashMap<>()).put(childKey, child);
        }
    }
}
