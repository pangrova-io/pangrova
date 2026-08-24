package org.pangrova.metadata.dynamictable;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory implementation of {@link RefreshHistoryStore} backed by
 * {@link ConcurrentHashMap} with per-dtName {@link CopyOnWriteArrayList}.
 *
 * <p>Concurrency strategy: ConcurrentHashMap provides thread-safe bucket
 * access; CopyOnWriteArrayList provides thread-safe iteration for queries
 * while allowing concurrent writes. Suitable for moderate write frequency
 * (refresh events) with higher read frequency (information_schema queries).</p>
 */
@ThreadSafe
public final class InMemoryRefreshHistoryStore implements RefreshHistoryStore {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryRefreshHistoryStore.class);

    private final Map<String, CopyOnWriteArrayList<RefreshHistoryEntry>> store =
            new ConcurrentHashMap<>();

    @Override
    public void record(RefreshHistoryEntry entry) {
        store.computeIfAbsent(entry.dtName(), k -> new CopyOnWriteArrayList<>())
                .add(entry);
        LOG.info("action=refresh_history_record dtName={} refreshId={} status={}",
                entry.dtName(), entry.refreshId(), entry.status());
    }

    @Override
    public List<RefreshHistoryEntry> query(String dtName, Instant from, Instant to, int limit) {
        CopyOnWriteArrayList<RefreshHistoryEntry> entries = store.get(dtName);
        if (entries == null) {
            return List.of();
        }
        return entries.stream()
                .filter(e -> !e.startTime().isBefore(from) && !e.startTime().isAfter(to))
                .sorted(Comparator.comparing(RefreshHistoryEntry::startTime).reversed())
                .limit(limit)
                .toList();
    }

    @Override
    public void purge(Instant olderThan, int maxEntries) {
        for (Map.Entry<String, CopyOnWriteArrayList<RefreshHistoryEntry>> bucket : store.entrySet()) {
            CopyOnWriteArrayList<RefreshHistoryEntry> entries = bucket.getValue();
            purgeEntries(entries, olderThan, maxEntries);
        }
    }

    private void purgeEntries(
            CopyOnWriteArrayList<RefreshHistoryEntry> entries,
            Instant olderThan,
            int maxEntries) {

        // Remove entries older than the cutoff
        entries.removeIf(e -> e.startTime().isBefore(olderThan));

        // Trim to maxEntries, keeping the most recent
        if (entries.size() > maxEntries) {
            List<RefreshHistoryEntry> sorted = entries.stream()
                    .sorted(Comparator.comparing(RefreshHistoryEntry::startTime).reversed())
                    .toList();
            List<RefreshHistoryEntry> toRemove = sorted.subList(maxEntries, sorted.size());
            entries.removeAll(toRemove);
        }
    }
}
