package org.pangrova.worker.bootstrap;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.common.concurrent.NotThreadSafe;

/** Owns discovered function resources until each actual close succeeds. */
@NotThreadSafe
final class NativeFunctionResources {
    private Entry head;
    private AutoCloseable pending;

    void retain(AutoCloseable resource) {
        java.util.Objects.requireNonNull(resource, "resource");
        if (pending != null) {
            throw new IllegalStateException("Function resource publication has not completed");
        }
        // Publish before allocating the ledger entry. If allocation fails the
        // discovered resource remains owned and can still be closed on rollback.
        pending = resource;
        head = new Entry(resource, head);
        pending = null;
    }

    List<AutoCloseable> snapshot() {
        List<AutoCloseable> resources = new ArrayList<>();
        for (Entry entry = head; entry != null; entry = entry.next) resources.add(entry.resource);
        java.util.Collections.reverse(resources);
        if (pending != null) {
            resources.add(pending);
        }
        return List.copyOf(resources);
    }

    void close() {
        Throwable failure = null;
        if (pending != null) {
            try {
                pending.close();
                pending = null;
            } catch (Exception | Error closeFailure) {
                failure = closeFailure;
            }
        }
        Entry previous = null;
        Entry current = head;
        while (current != null) {
            try {
                current.resource.close();
                if (previous == null) {
                    head = current.next;
                } else {
                    previous.next = current.next;
                }
            } catch (Exception | Error closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else if (failure != closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                previous = current;
            }
            current = current.next;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("Failed to close native function resource", failure);
        }
    }

    private static final class Entry {
        private final AutoCloseable resource;
        private Entry next;

        private Entry(AutoCloseable resource, Entry next) {
            this.resource = resource;
            this.next = next;
        }
    }
}
