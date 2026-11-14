package org.pangrova.governance.runtime.audit;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.governance.api.audit.GovernanceAuditEntry;
import org.pangrova.governance.api.audit.GovernanceAuditTrail;
import org.pangrova.governance.api.enums.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe in-memory implementation of the governance audit trail.
 * Entries are append-only (immutable once recorded) and stored in insertion order.
 * Suitable for development, testing, and single-node deployments.
 */
@ThreadSafe
public final class InMemoryGovernanceAuditTrail implements GovernanceAuditTrail {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryGovernanceAuditTrail.class);

    private final List<GovernanceAuditEntry> entries = new ArrayList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong idGenerator = new AtomicLong(1);

    @Override
    public GovernanceAuditEntry record(
            String actor,
            OperationType operationType,
            String resourcePath,
            Map<String, String> beforeState,
            Map<String, String> afterState,
            String details) {
        GovernanceAuditEntry entry = new GovernanceAuditEntry(
                idGenerator.getAndIncrement(),
                Instant.now(),
                actor,
                operationType,
                resourcePath,
                beforeState,
                afterState,
                details);
        lock.writeLock().lock();
        try {
            entries.add(entry);
        } finally {
            lock.writeLock().unlock();
        }
        LOG.info("event=governance_audit_recorded id={} actor={} type={} resource={}",
                entry.id(), actor, operationType, resourcePath);
        return entry;
    }

    @Override
    public List<GovernanceAuditEntry> getEntriesForResource(String resourcePath) {
        lock.readLock().lock();
        try {
            return entries.stream()
                    .filter(e -> e.resourcePath().equals(resourcePath))
                    .sorted((a, b) -> b.timestamp().compareTo(a.timestamp()))
                    .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<GovernanceAuditEntry> getEntriesByActor(String actor) {
        lock.readLock().lock();
        try {
            return entries.stream()
                    .filter(e -> e.actor().equals(actor))
                    .sorted((a, b) -> b.timestamp().compareTo(a.timestamp()))
                    .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<GovernanceAuditEntry> getEntriesInRange(Instant from, Instant to) {
        lock.readLock().lock();
        try {
            return entries.stream()
                    .filter(e -> !e.timestamp().isBefore(from) && e.timestamp().isBefore(to))
                    .sorted((a, b) -> b.timestamp().compareTo(a.timestamp()))
                    .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long entryCount() {
        lock.readLock().lock();
        try {
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
