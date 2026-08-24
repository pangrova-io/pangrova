package org.pangrova.metadata.dependency;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory {@link ObjectDependencyStore}.
 *
 * <p>Used for unit tests and embedded-mode deployments that do not back
 * metadata to a JDBC store. Production deployments should use
 * {@link JdbcObjectDependencyStore} so dependencies survive restarts and are
 * visible across coordinators.
 *
 * <p>The internal layout is a forward map (child -> parents) and a reverse
 * map (parent -> children). Both maps are kept in sync under a single
 * read-write lock; reads parallelize, writes serialize.
 */
public final class InMemoryObjectDependencyStore implements ObjectDependencyStore {

    private final Map<ObjectRef, Set<ObjectRef>> parentsByChild = new LinkedHashMap<>();
    private final Map<ObjectRef, Set<ObjectRef>> childrenByParent = new LinkedHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public void recordDependencies(ObjectRef child, Set<ObjectRef> parents) {
        Objects.requireNonNull(child, "child must not be null");
        Objects.requireNonNull(parents, "parents must not be null");
        if (parents.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            Set<ObjectRef> currentParents =
                    parentsByChild.computeIfAbsent(child, k -> ConcurrentHashMap.newKeySet());
            for (ObjectRef parent : parents) {
                if (parent.equals(child)) {
                    continue; // skip self-edges, mirrors JDBC behavior
                }
                if (currentParents.add(parent)) {
                    childrenByParent
                            .computeIfAbsent(parent, k -> ConcurrentHashMap.newKeySet())
                            .add(child);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void replaceDependenciesForChild(ObjectRef child, Set<ObjectRef> parents) {
        Objects.requireNonNull(child, "child must not be null");
        Objects.requireNonNull(parents, "parents must not be null");
        lock.writeLock().lock();
        try {
            Set<ObjectRef> existing = parentsByChild.remove(child);
            if (existing != null) {
                for (ObjectRef oldParent : existing) {
                    removeChildFromParent(oldParent, child);
                }
            }
            if (!parents.isEmpty()) {
                recordDependenciesUnsafe(child, parents);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Removes a child from a parent's children set, cleaning up empty entries. */
    private void removeChildFromParent(ObjectRef parent, ObjectRef child) {
        Set<ObjectRef> kids = childrenByParent.get(parent);
        if (kids != null) {
            kids.remove(child);
            if (kids.isEmpty()) {
                childrenByParent.remove(parent);
            }
        }
    }

    @Override
    public int removeAllEdgesFor(ObjectRef obj) {
        Objects.requireNonNull(obj, "obj must not be null");
        lock.writeLock().lock();
        try {
            // Edges where obj is the child:
            Set<ObjectRef> parents = parentsByChild.remove(obj);
            int removed = removeFromReverseMap(parents, obj, childrenByParent);
            // Edges where obj is the parent:
            Set<ObjectRef> children = childrenByParent.remove(obj);
            removed += removeFromReverseMap(children, obj, parentsByChild);
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Removes {@code target} from each set in the reverse map, cleaning up empty entries. */
    private static int removeFromReverseMap(Set<ObjectRef> keys, ObjectRef target,
                                            Map<ObjectRef, Set<ObjectRef>> reverseMap) {
        if (keys == null) {
            return 0;
        }
        int removed = 0;
        for (ObjectRef key : keys) {
            Set<ObjectRef> entries = reverseMap.get(key);
            if (entries != null && entries.remove(target)) {
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
            Set<ObjectRef> parents = parentsByChild.get(child);
            return parents == null ? List.of() : List.copyOf(parents);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<ObjectRef> getDependentsOf(ObjectRef parent) {
        Objects.requireNonNull(parent, "parent must not be null");
        lock.readLock().lock();
        try {
            Set<ObjectRef> children = childrenByParent.get(parent);
            return children == null ? List.of() : List.copyOf(children);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<ObjectRef> getTransitiveDependentsOf(ObjectRef parent) {
        Objects.requireNonNull(parent, "parent must not be null");
        lock.readLock().lock();
        try {
            Set<ObjectRef> visited = new HashSet<>();
            Deque<ObjectRef> frontier = new ArrayDeque<>();
            List<ObjectRef> order = new ArrayList<>();
            frontier.add(parent);
            visited.add(parent);
            while (!frontier.isEmpty()) {
                ObjectRef current = frontier.poll();
                Set<ObjectRef> kids = childrenByParent.get(current);
                if (kids == null) {
                    continue;
                }
                for (ObjectRef child : kids) {
                    if (visited.add(child)) {
                        order.add(child);
                        frontier.add(child);
                    }
                }
            }
            Collections.reverse(order);
            return order;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void recordDependenciesUnsafe(ObjectRef child, Set<ObjectRef> parents) {
        Set<ObjectRef> currentParents =
                parentsByChild.computeIfAbsent(child, k -> new LinkedHashSet<>());
        for (ObjectRef parent : parents) {
            if (parent.equals(child)) {
                continue;
            }
            if (currentParents.add(parent)) {
                childrenByParent
                        .computeIfAbsent(parent, k -> new LinkedHashSet<>())
                        .add(child);
            }
        }
    }
}
