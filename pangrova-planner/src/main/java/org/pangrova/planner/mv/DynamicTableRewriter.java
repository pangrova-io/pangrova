package org.pangrova.planner.mv;

import org.pangrova.parser.ast.RefreshStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import java.util.List;

/**
 * Resolves Dynamic Table names to their physical target tables ({@code __dt_NAME}).
 *
 * <p>A Dynamic Table is identified by having {@link RefreshStrategy#INCREMENTAL}
 * in its {@link MaterializedViewDefinition}. The rewriter checks queryability
 * via {@link MaterializedViewDefinition#enableQueryRewrite()} and either returns
 * the target table name or throws {@link DynamicTableNotReadyException}.</p>
 *
 * <p>Supports explicit target overrides via {@link #registerTargetOverride(String, String)}.
 * When an override is registered, the DT resolves to the override target instead
 * of the default {@code __dt_NAME}. This enables pass-through resolution
 * (e.g. CDC mode DTs resolving directly to the source table).</p>
 *
 * <p>When a {@link SnapshotInfoProvider} is wired, the rewriter compares the
 * DT's last refresh snapshot against the source table's current snapshot to
 * determine whether a merge-on-read UNION ALL rewrite is needed.</p>
 *
 * <p>Called from {@link org.pangrova.planner.PangrovaPlanner#plan(String)} before
 * Calcite table resolution to transparently redirect DT queries.</p>
 */
@org.pangrova.common.concurrent.ThreadSafe
public final class DynamicTableRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableRewriter.class);
    static final String TARGET_PREFIX = "__dt_";

    private final MaterializedViewRegistry materializedViewManager;
    private final SnapshotInfoProvider snapshotInfoProvider;
    private final PublishedTargetResolver publishedTargetResolver;
    private final Map<String, String> targetOverrides = new ConcurrentHashMap<>();

    /**
     * Creates a rewriter without merge-on-read support (backward compatible).
     * All resolutions return {@link DynamicTableResolution.DirectTarget}.
     */
    public DynamicTableRewriter(MaterializedViewRegistry materializedViewManager) {
        this(materializedViewManager, null);
    }

    /**
     * Creates a rewriter with optional merge-on-read support.
     *
     * @param materializedViewManager DT definition registry
     * @param snapshotInfoProvider    snapshot info for merge-on-read resolution (nullable)
     */
    public DynamicTableRewriter(MaterializedViewRegistry materializedViewManager,
                                SnapshotInfoProvider snapshotInfoProvider) {
        this.materializedViewManager = Objects.requireNonNull(materializedViewManager, "materializedViewManager");
        this.snapshotInfoProvider = snapshotInfoProvider;
        this.publishedTargetResolver = null;
    }

    /** Resolves only durable, published physical generations for the current session. */
    public DynamicTableRewriter(PublishedTargetResolver publishedTargetResolver) {
        this.materializedViewManager = null;
        this.snapshotInfoProvider = null;
        this.publishedTargetResolver = Objects.requireNonNull(publishedTargetResolver,
                "publishedTargetResolver");
    }

    /** Component-preserving query-local authority for one published generation. */
    public record PublishedTarget(QualifiedTableName logical, QualifiedTableName physical) { }

    /** Resolves an AST identifier without flattening quoted components. */
    @FunctionalInterface
    public interface PublishedTargetResolver {
        Optional<PublishedTarget> resolve(List<String> nameParts);

        /** Qualifies an AST name without reading a publication, for query-local pin lookup. */
        default Optional<QualifiedTableName> canonicalLogical(List<String> nameParts) {
            return Optional.empty();
        }
    }

    /** Returns both identities for an AST reference in the durable publication mode. */
    public Optional<PublishedTarget> resolvePublishedTarget(List<String> nameParts) {
        if (publishedTargetResolver == null) {
            throw new IllegalStateException("Published target resolver is not wired");
        }
        return publishedTargetResolver.resolve(List.copyOf(nameParts));
    }

    /** Returns the durable logical identity before a publication read, when available. */
    public Optional<QualifiedTableName> canonicalPublishedLogical(List<String> nameParts) {
        if (publishedTargetResolver == null) {
            throw new IllegalStateException("Published target resolver is not wired");
        }
        return publishedTargetResolver.canonicalLogical(List.copyOf(nameParts));
    }

    /** Whether this instance uses the historical in-memory definition registry. */
    public boolean usesRegistry() {
        return publishedTargetResolver == null;
    }

    /**
     * Register an explicit target override for a Dynamic Table.
     * When set, {@link #resolveTargetTable(String)} returns the override
     * instead of the default {@code __dt_NAME} prefix.
     *
     * @param dtName the Dynamic Table name
     * @param targetTable the resolved target table name (e.g. source table for CDC pass-through)
     */
    public void registerTargetOverride(String dtName, String targetTable) {
        targetOverrides.put(dtName, targetTable);
        LOG.info("event=dt_target_override_registered dtName={} target={}", dtName, targetTable);
    }

    /**
     * Remove a target override for a Dynamic Table.
     *
     * @param dtName the Dynamic Table name
     */
    public void removeTargetOverride(String dtName) {
        targetOverrides.remove(dtName);
    }

    /**
     * Returns all DT names that have registered target overrides.
     * Used by the fallback rewrite path when AST parsing fails.
     */
    public java.util.Set<String> getRegisteredOverrideNames() {
        return java.util.Set.copyOf(targetOverrides.keySet());
    }

    /**
     * Resolve a table name to its DT target table if applicable.
     *
     * <p>Resolution order: explicit override → default {@code __dt_NAME} prefix.</p>
     *
     * @param tableName the table name from the query
     * @return target table name if this is a queryable DT, empty if not a DT
     * @throws DynamicTableNotReadyException if DT exists but initial refresh
     *         has not completed ({@code enableQueryRewrite == false})
     */
    public Optional<String> resolveTargetTable(String tableName) {
        DynamicTableResolution resolution = resolve(tableName);
        if (resolution == null) {
            return Optional.empty();
        }
        // For SQL-level rewrite, always return the materialized table name.
        // MergeOnRead UNION ALL generation happens at the plan level, not SQL level.
        if (resolution instanceof DynamicTableResolution.DirectTarget dt) {
            return Optional.of(dt.tableName());
        }
        if (resolution instanceof DynamicTableResolution.MergeOnRead mor) {
            return Optional.of(mor.materializedTable());
        }
        return Optional.empty();
    }

    /**
     * Resolve a table name to a {@link DynamicTableResolution} describing how to query it.
     *
     * <p>When a {@link SnapshotInfoProvider} is wired and the DT has pending deltas
     * (lastRefreshSnapshot &lt; currentSnapshot), returns {@link DynamicTableResolution.MergeOnRead}
     * so the planner can generate a UNION ALL plan. Otherwise returns
     * {@link DynamicTableResolution.DirectTarget}.</p>
     *
     * @param tableName the table name from the query
     * @return resolution describing how to query this DT, or null if not a DT
     * @throws DynamicTableNotReadyException if DT exists but initial refresh
     *         has not completed
     */
    public DynamicTableResolution resolve(String tableName) {
        if (publishedTargetResolver != null) {
            return publishedTargetResolver.resolve(List.of(tableName))
                    .<DynamicTableResolution>map(target ->
                            new DynamicTableResolution.DirectTarget(target.physical().toString()))
                    .orElse(null);
        }
        String normalized = tableName.toLowerCase(java.util.Locale.ROOT);
        Optional<MaterializedViewDefinition> def = materializedViewManager.getDefinition(normalized);
        if (def.isEmpty() || def.get().refreshStrategy() != RefreshStrategy.INCREMENTAL) {
            return null;
        }
        if (!def.get().enableQueryRewrite()) {
            throw new DynamicTableNotReadyException(tableName);
        }
        String target = targetOverrides.getOrDefault(normalized, TARGET_PREFIX + normalized);

        // Attempt merge-on-read resolution when snapshot info is available
        if (snapshotInfoProvider != null) {
            DynamicTableResolution mergeOnRead = tryMergeOnRead(normalized, target, def.get());
            if (mergeOnRead != null) {
                return mergeOnRead;
            }
        }

        LOG.info("event=dt_query_rewrite dtName={} target={} resolution=direct", normalized, target);
        return new DynamicTableResolution.DirectTarget(target);
    }

    private DynamicTableResolution tryMergeOnRead(String dtName, String target,
                                                   MaterializedViewDefinition def) {
        try {
            OptionalLong lastRefresh = snapshotInfoProvider.lastRefreshSnapshotId(dtName);
            if (lastRefresh.isEmpty()) {
                return null;
            }
            String sourceTable = snapshotInfoProvider.sourceTable(dtName);
            if (sourceTable == null) {
                return null;
            }
            OptionalLong current = snapshotInfoProvider.currentSnapshotId(sourceTable);
            if (current.isEmpty()) {
                return null;
            }
            long lastRefreshId = lastRefresh.getAsLong();
            long currentId = current.getAsLong();
            if (lastRefreshId >= currentId) {
                // Fully refreshed — no pending deltas
                return null;
            }
            LOG.info("event=dt_query_rewrite dtName={} resolution=merge_on_read "
                            + "lastRefreshSnapshot={} currentSnapshot={}",
                    dtName, lastRefreshId, currentId);
            return new DynamicTableResolution.MergeOnRead(target, lastRefreshId, sourceTable, currentId);
        } catch (Exception e) {
            LOG.warn("event=dt_merge_on_read_check_failed dtName={} error={} action=fallback_to_direct",
                    dtName, e.getMessage());
            return null;
        }
    }
}
