package org.pangrova.worker.ai.blob;

import org.pangrova.spi.ai.embedding.BlobReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optimizes blob content reads for Late Materialization queries by batching
 * prefetch requests for rows that have passed filter evaluation.
 *
 * <p><b>Problem:</b> Without prefetching, each row's AI function evaluation
 * sequentially reads blob content from external storage, causing N round-trips
 * for N qualifying rows.</p>
 *
 * <p><b>Solution:</b> This optimizer collects BlobReferences from qualifying rows
 * after filter evaluation, issues parallel prefetch requests, and provides
 * pre-fetched content to downstream AI functions.</p>
 *
 * <p><b>Usage pattern:</b></p>
 * <pre>
 * // After Velox scan + filter produces qualifying rows:
 * BlobPrefetchOptimizer prefetcher = new BlobPrefetchOptimizer(reader, executor);
 * prefetcher.submitPrefetch(qualifyingBlobRefs);
 *
 * // During AI function evaluation:
 * byte[] content = prefetcher.getOrRead(blobRef);  // returns prefetched if available
 * </pre>
 *
 * <p>Thread safety: this class is designed for single-thread prefetch submission
 * followed by multi-thread content retrieval (safe for the Velox batch evaluation
 * pattern where submission happens in the scan thread and retrieval happens in
 * the UDF evaluation thread).</p>
 */
public final class BlobPrefetchOptimizer {

    private static final Logger LOG = LoggerFactory.getLogger(BlobPrefetchOptimizer.class);

    /** Default maximum concurrent prefetch requests. */
    static final int DEFAULT_MAX_CONCURRENT = 8;

    /** Default maximum prefetch batch size before flushing. */
    static final int DEFAULT_PREFETCH_BATCH_SIZE = 32;

    /** Maximum total bytes to prefetch in memory (256 MB default). */
    static final long DEFAULT_MAX_PREFETCH_BYTES = 256L * 1024 * 1024;

    private final BlobContentReader contentReader;
    private final ExecutorService prefetchExecutor;
    private final int maxConcurrent;
    private final long maxPrefetchBytes;

    /** Prefetched content cache: URI → future of content bytes. */
    private final Map<String, CompletableFuture<byte[]>> prefetchCache = new HashMap<>();

    /** Total bytes currently in prefetch cache (for memory pressure tracking). */
    private final AtomicLong cachedBytes = new AtomicLong(0);

    /**
     * Creates a prefetch optimizer with default settings.
     *
     * @param contentReader  blob content reader for actual storage access
     * @param prefetchExecutor thread pool for parallel prefetch I/O
     */
    public BlobPrefetchOptimizer(BlobContentReader contentReader, ExecutorService prefetchExecutor) {
        this(contentReader, prefetchExecutor, DEFAULT_MAX_CONCURRENT, DEFAULT_MAX_PREFETCH_BYTES);
    }

    /**
     * Creates a prefetch optimizer with custom concurrency and memory limits.
     *
     * @param contentReader    blob content reader
     * @param prefetchExecutor thread pool for parallel prefetch I/O
     * @param maxConcurrent    maximum number of concurrent prefetch requests
     * @param maxPrefetchBytes maximum total bytes to hold in prefetch cache
     */
    public BlobPrefetchOptimizer(BlobContentReader contentReader,
                                  ExecutorService prefetchExecutor,
                                  int maxConcurrent,
                                  long maxPrefetchBytes) {
        this.contentReader = Objects.requireNonNull(contentReader, "contentReader must not be null");
        this.prefetchExecutor = Objects.requireNonNull(prefetchExecutor, "prefetchExecutor must not be null");
        this.maxConcurrent = maxConcurrent > 0 ? maxConcurrent : DEFAULT_MAX_CONCURRENT;
        this.maxPrefetchBytes = maxPrefetchBytes > 0 ? maxPrefetchBytes : DEFAULT_MAX_PREFETCH_BYTES;
    }

    /**
     * Submits a batch of blob references for asynchronous prefetching.
     *
     * <p>References whose content is already cached are skipped. When the
     * memory limit is reached, no further prefetches are issued.</p>
     *
     * @param refs list of blob references to prefetch (null entries are ignored)
     */
    public void submitPrefetch(List<BlobReference> refs) {
        if (refs == null || refs.isEmpty()) {
            return;
        }

        int submitted = 0;
        for (BlobReference ref : refs) {
            if (cachedBytes.get() >= maxPrefetchBytes) {
                LOG.debug("event=prefetch_memory_limit_reached cached_bytes={} max={}",
                        cachedBytes.get(), maxPrefetchBytes);
                break;
            }
            if (isSkippable(ref)) {
                continue; // Null entry or already prefetching/cached
            }
            if (submitted >= maxConcurrent) {
                break; // Concurrency limit
            }

            submitOne(ref);
            submitted++;
        }

        if (submitted > 0) {
            LOG.debug("event=prefetch_submitted count={} total_cached={}", submitted, prefetchCache.size());
        }
    }

    private boolean isSkippable(BlobReference ref) {
        return ref == null || prefetchCache.containsKey(ref.uri());
    }

    private void submitOne(BlobReference ref) {
        CompletableFuture<byte[]> future = CompletableFuture.supplyAsync(
                () -> {
                    byte[] content = contentReader.read(ref);
                    cachedBytes.addAndGet(content.length);
                    return content;
                },
                prefetchExecutor);
        prefetchCache.put(ref.uri(), future);
    }

    /**
     * Retrieves blob content, using prefetched data if available.
     *
     * <p>If the blob was previously submitted for prefetch, returns the
     * prefetched content (blocking if not yet complete). Otherwise, reads
     * directly from storage (fallback path).</p>
     *
     * @param ref blob reference to read
     * @return raw blob content bytes
     */
    public byte[] getOrRead(BlobReference ref) {
        Objects.requireNonNull(ref, "ref must not be null");

        CompletableFuture<byte[]> cached = prefetchCache.get(ref.uri());
        if (cached != null) {
            try {
                byte[] content = cached.join();
                LOG.debug("event=prefetch_hit uri={}", ref.uri());
                return content;
            } catch (Exception e) {
                LOG.warn("event=prefetch_failed uri={} error={}", ref.uri(), e.getMessage());
                // Fall through to direct read
            }
        }

        // Direct read fallback
        return contentReader.read(ref);
    }

    /**
     * Returns the number of entries currently in the prefetch cache.
     */
    public int cacheSize() {
        return prefetchCache.size();
    }

    /**
     * Returns the approximate total bytes in the prefetch cache.
     */
    public long cachedBytes() {
        return cachedBytes.get();
    }

    /**
     * Clears the prefetch cache and resets memory tracking.
     * Call between batches to release memory.
     */
    public void clear() {
        prefetchCache.clear();
        cachedBytes.set(0);
    }

    /**
     * Extracts BlobReferences from a batch of FILE struct column values.
     *
     * <p>Utility method for use with Late Materialization: after filtering,
     * call this to collect all blob references from qualifying rows, then
     * submit them for prefetch.</p>
     *
     * @param fileStructValues list of FILE struct field maps (one per qualifying row);
     *                         null entries represent NULL FILE columns
     * @return list of non-null BlobReferences extracted from the structs
     */
    public static List<BlobReference> extractReferences(List<Map<String, String>> fileStructValues) {
        if (fileStructValues == null || fileStructValues.isEmpty()) {
            return List.of();
        }

        List<BlobReference> refs = new ArrayList<>();
        for (Map<String, String> fields : fileStructValues) {
            if (fields == null) {
                continue;
            }
            if (isComplete(fields)) {
                addReference(refs, fields);
            }
        }
        return Collections.unmodifiableList(refs);
    }

    private static boolean isComplete(Map<String, String> fields) {
        String uri = fields.get("uri");
        String mimeType = fields.get("mime_type");
        String checksum = fields.get("checksum");
        return uri != null && !uri.isBlank()
                && mimeType != null && !mimeType.isBlank()
                && fields.get("size") != null && checksum != null && !checksum.isBlank();
    }

    private static void addReference(List<BlobReference> refs, Map<String, String> fields) {
        try {
            long size = Long.parseLong(fields.get("size"));
            refs.add(new BlobReference(fields.get("uri"), fields.get("mime_type"), size, fields.get("checksum")));
        } catch (NumberFormatException ignored) {
            // Skip malformed entries
        }
    }
}
