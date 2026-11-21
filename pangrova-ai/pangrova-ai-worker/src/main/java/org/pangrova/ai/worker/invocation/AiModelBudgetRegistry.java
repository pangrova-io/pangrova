package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.enums.AiFailureCategory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.pangrova.ai.api.exception.AiInvocationException;

/** Shares one concurrency semaphore per live exact model deployment. */
final class AiModelBudgetRegistry {

    private final ConcurrentHashMap<ModelKey, Entry> entries = new ConcurrentHashMap<>();

    Lease retain(
            String modelName,
            String modelVersion,
            String signatureSha256,
            String capabilityId,
            int maxConcurrency) {
        ModelKey key = new ModelKey(
                modelName, modelVersion, signatureSha256, capabilityId);
        Entry entry = entries.compute(key, (ignored, current) -> {
            Entry selected = current == null ? new Entry(maxConcurrency) : current;
            selected.requireSameLimit(maxConcurrency);
            selected.references++;
            return selected;
        });
        return new Lease(key, entry);
    }

    int size() {
        return entries.size();
    }

    private void release(ModelKey key, Entry expected) {
        entries.compute(key, (ignored, current) -> {
            if (current != expected || current.references <= 0) {
                throw new IllegalStateException("AI model budget lease was not retained");
            }
            current.references--;
            return current.references == 0 ? null : current;
        });
    }

    private record ModelKey(
            String modelName,
            String modelVersion,
            String signatureSha256,
            String capabilityId) {}

    private static final class Entry {
        private final int limit;
        private final Semaphore concurrency;
        private int references;

        private Entry(int limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("model concurrency must be positive");
            }
            this.limit = limit;
            this.concurrency = new Semaphore(limit, true);
        }

        private void requireSameLimit(int expected) {
            if (limit != expected) {
                throw new AiInvocationException(
                        AiFailureCategory.CAPABILITY_MISMATCH,
                        false,
                        "Exact model deployment changed its concurrency envelope");
            }
        }
    }

    final class Lease implements AutoCloseable {
        private final ModelKey key;
        private final Entry entry;
        private boolean closed;

        private Lease(ModelKey key, Entry entry) {
            this.key = key;
            this.entry = entry;
        }

        Semaphore concurrency() {
            return entry.concurrency;
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                release(key, entry);
                closed = true;
            }
        }
    }
}
