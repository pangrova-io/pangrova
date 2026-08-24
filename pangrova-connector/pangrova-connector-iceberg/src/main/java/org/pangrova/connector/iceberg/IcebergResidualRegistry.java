package org.pangrova.connector.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * In-process registry mapping data file path -&gt; Iceberg residual {@link Expression}
 * for use by the Java Parquet fallback reader.
 *
 * <p>Iceberg's {@code FileScanTask.residual()} produces a per-file expression that
 * survives partition + file-level (manifest) pruning. Passing this residual to
 * {@code Parquet.read().filter(expression)} enables Parquet's native
 * row-group / page-index pruning, which is otherwise off in the Java fallback path.
 *
 * <p>Why a registry instead of a field on {@code IcebergSplit}? The split record is
 * proto-serialized for native dispatch — Iceberg's {@code Expression} is not
 * proto-serializable, and the native engine consumes a Substrait-encoded copy of
 * the same predicate (see {@code IcebergSplit#residualPredicate}). The registry
 * keeps the Java-only {@code Expression} alongside the split for in-process use,
 * exactly mirroring {@link PaimonSplitRegistry}.
 *
 * <p>Backed by Caffeine with a 30-minute idle TTL and 100k-entry ceiling so a
 * cancelled or abandoned plan doesn't leak. Cleared on connector shutdown.
 *
 * <p>Thread-safe.
 */
public final class IcebergResidualRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergResidualRegistry.class);

    private static final int DEFAULT_MAX_SIZE = 100_000;
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(30);

    private final Cache<String, Expression> residuals;

    public IcebergResidualRegistry() {
        this(DEFAULT_MAX_SIZE, DEFAULT_TTL);
    }

    IcebergResidualRegistry(int maxSize, Duration ttl) {
        this.residuals = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(ttl)
                .evictionListener((key, value, cause) ->
                        LOG.debug("event=residual_evicted file={} cause={}", key, cause))
                .build();
    }

    /**
     * Registers a residual expression for the given data file path.
     * No-op if the expression is null or {@code Expressions.alwaysTrue()}.
     */
    public void register(String filePath, Expression residual) {
        if (filePath == null || filePath.isEmpty() || residual == null) {
            return;
        }
        if (residual instanceof org.apache.iceberg.expressions.True) {
            return;
        }
        residuals.put(filePath, residual);
    }

    /**
     * Returns the residual expression for the given file path, or
     * {@code Expressions.alwaysTrue()} (no-op filter) when none is registered.
     * The entry remains until evicted by TTL — multiple readers for the same
     * file (e.g. retries) reuse the same expression.
     */
    public Expression find(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return Expressions.alwaysTrue();
        }
        Expression expr = residuals.getIfPresent(filePath);
        return expr != null ? expr : Expressions.alwaysTrue();
    }

    /** Number of entries currently held — for monitoring / leak detection. */
    public int size() {
        return (int) residuals.estimatedSize();
    }

    /** Removes all registered entries. Called on connector shutdown. */
    public void clear() {
        residuals.invalidateAll();
    }
}
