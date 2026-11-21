package org.pangrova.persistence.catalog.api.dependency;

import org.pangrova.metadata.api.relation.ObjectRef;

import org.pangrova.common.api.annotation.Internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Domain ordering shared by stores; callers retain their own locking and connection ownership. */
@Internal
public final class ObjectDependencyOrder {
    private ObjectDependencyOrder() {}

    /**
     * Returns each reachable dependent after its descendants, omitting the root identity.
     * Natural names are visited once, independent of the kind stored on each edge.
     * Revisited nodes are skipped, so malformed cycles terminate without duplication.
     * A cyclic component cannot have a topological order; no such order is promised for it.
     */
    public static List<ObjectRef> dependentsFirst(ObjectRef root,
            Function<ObjectRef, List<ObjectRef>> children) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(children, "children");
        Set<ObjectDependencyKey> visited = new HashSet<>();
        Deque<Frame> stack = new ArrayDeque<>();
        List<ObjectRef> order = new ArrayList<>();
        ObjectDependencyKey rootKey = ObjectDependencyKey.of(root);
        visited.add(rootKey);
        stack.push(new Frame(root, children.apply(root).iterator()));
        while (!stack.isEmpty()) {
            Frame frame = stack.peek();
            if (frame.children().hasNext()) {
                ObjectRef child = frame.children().next();
                if (visited.add(ObjectDependencyKey.of(child))) {
                    stack.push(new Frame(child, children.apply(child).iterator()));
                }
            } else {
                stack.pop();
                if (!ObjectDependencyKey.of(frame.object()).equals(rootKey)) order.add(frame.object());
            }
        }
        return order;
    }

    private record Frame(ObjectRef object, Iterator<ObjectRef> children) {}
}
