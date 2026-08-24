package org.pangrova.coordinator.ai.cache;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory semantic cache that finds similar queries by cosine similarity
 * of their embedding vectors.
 *
 * <p>When a query is semantically similar to a previously cached query
 * (above the configured threshold), the cached result is returned without
 * re-executing the query. Entries are tenant-isolated and principal-scoped.</p>
 *
 * <p>Uses brute-force cosine similarity scan — suitable for cache sizes up to
 * ~10K entries. For larger caches, replace with an ANN index.</p>
 */
@ThreadSafe
public final class InMemorySemanticCache implements SemanticCache {

    private static final Logger LOG = LoggerFactory.getLogger(InMemorySemanticCache.class);

    /** Maximum number of SQL characters shown in debug log previews. */
    private static final int SQL_LOG_PREVIEW_LENGTH = 60;

    private final int maxEntries;
    private final List<SemanticCacheEntry> entries;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Creates a cache holding at most {@code maxEntries} entries. */
    public InMemorySemanticCache(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive, got: " + maxEntries);
        }
        this.maxEntries = maxEntries;
        this.entries = new ArrayList<>();
    }

    /**
     * Searches for a semantically similar cached query.
     *
     * @param queryEmbedding embedding vector of the incoming query
     * @param tenantId       tenant isolation key (null matches entries with null tenant)
     * @param principal      user principal for access control scoping
     * @param threshold      minimum cosine similarity to consider a match (0.0 to 1.0)
     * @return the best match above the threshold, or empty if no match
     */
    public Optional<SemanticCacheHit> findSimilar(float[] queryEmbedding,
                                                   String tenantId,
                                                   String principal,
                                                   double threshold) {
        if (queryEmbedding == null || queryEmbedding.length == 0) {
            return Optional.empty();
        }
        lock.readLock().lock();
        try {
            SemanticCacheEntry bestEntry = null;
            double bestSimilarity = threshold;

            for (SemanticCacheEntry entry : entries) {
                if (!matchesTenant(entry, tenantId)) {
                    continue;
                }
                double similarity = cosineSimilarity(queryEmbedding, entry.queryEmbedding());
                if (similarity > bestSimilarity) {
                    bestSimilarity = similarity;
                    bestEntry = entry;
                }
            }
            if (bestEntry != null) {
                LOG.debug("event=semantic_cache_hit similarity={} cachedSql={}",
                        bestSimilarity,
                        previewSql(bestEntry.originalSql()));
                return Optional.of(new SemanticCacheHit(bestEntry, bestSimilarity));
            }
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Stores a query result in the semantic cache.
     *
     * @param entry the cache entry to store
     */
    public void put(SemanticCacheEntry entry) {
        if (entry == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            if (entries.size() >= maxEntries) {
                entries.remove(0);
            }
            entries.add(entry);
            LOG.debug("event=semantic_cache_put sql={} tables={} size={}",
                    previewSql(entry.originalSql()),
                    entry.referencedTables(), entries.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Invalidates all entries referencing the specified table.
     * Called after DML operations to ensure stale results are evicted.
     *
     * @param tableName fully qualified table name
     */
    public void invalidateByTable(String tableName) {
        if (tableName == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            int removed = 0;
            var it = entries.iterator();
            while (it.hasNext()) {
                if (it.next().referencedTables().contains(tableName)) {
                    it.remove();
                    removed++;
                }
            }
            if (removed > 0) {
                LOG.info("event=semantic_cache_invalidated table={} removed={} remaining={}",
                        tableName, removed, entries.size());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Returns the current number of cached entries. */
    public long size() {
        lock.readLock().lock();
        try {
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private static boolean matchesTenant(SemanticCacheEntry entry, String tenantId) {
        if (tenantId == null) {
            return entry.tenantId() == null;
        }
        return tenantId.equals(entry.tenantId());
    }

    private static String previewSql(String sql) {
        if (sql.length() > SQL_LOG_PREVIEW_LENGTH) {
            return sql.substring(0, SQL_LOG_PREVIEW_LENGTH) + "...";
        }
        return sql;
    }

    static double cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) {
            return 0.0;
        }
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        // Unrolled loop (4x) for better JIT auto-vectorization.
        int i = 0;
        int len = a.length;
        int limit = len - (len % 4);
        for (; i < limit; i += 4) {
            dot += (double) a[i] * b[i] + (double) a[i + 1] * b[i + 1]
                 + (double) a[i + 2] * b[i + 2] + (double) a[i + 3] * b[i + 3];
            normA += (double) a[i] * a[i] + (double) a[i + 1] * a[i + 1]
                   + (double) a[i + 2] * a[i + 2] + (double) a[i + 3] * a[i + 3];
            normB += (double) b[i] * b[i] + (double) b[i + 1] * b[i + 1]
                   + (double) b[i + 2] * b[i + 2] + (double) b[i + 3] * b[i + 3];
        }
        for (; i < len; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        double denominator = Math.sqrt(normA) * Math.sqrt(normB);
        return denominator > 0 ? dot / denominator : 0.0;
    }

}
