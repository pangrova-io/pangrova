package org.pangrova.resource.memory;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Leaf-level memory pool for a single operator within a task.
 * Reserves its quota from the parent {@link TaskMemoryPool}.
 */
@ThreadSafe
public final class OperatorMemoryPool extends MemoryPool {

    private final TaskMemoryPool parent;

    OperatorMemoryPool(String name, long maxBytes, TaskMemoryPool parent) {
        super(name, maxBytes);
        this.parent = parent;
    }

    public TaskMemoryPool getParent() {
        return parent;
    }
}
