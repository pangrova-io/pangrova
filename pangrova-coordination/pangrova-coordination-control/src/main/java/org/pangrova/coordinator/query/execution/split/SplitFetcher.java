package org.pangrova.coordinator.query.execution.split;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.protocol.index.IndexEvalHintProtoCodec;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.SplitPlanningBudget;
import org.pangrova.connector.api.metadata.SplitPlanningBudget.FileGranularity;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.indexing.api.evaluation.IndexEvaluatedSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches splits from connector catalogs based on extracted table scan descriptors.
 *
 * <p>This class is Calcite-free: all plan analysis and predicate extraction is
 * performed by {@link org.pangrova.planner.analysis.PlanSplitSourceExtractor} in the planner
 * module. SplitFetcher works exclusively with the Calcite-free
 * {@link SplitSourceDescriptor} representation.</p>
 */
public final class SplitFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(SplitFetcher.class);

    /** Maximum time to resolve and enumerate all sources of a stage. */
    private static final long SPLIT_FETCH_TIMEOUT_MS = SplitPlanningBudget.DEFAULT_TIMEOUT_MS;

    /** Maximum number of splits per stage to prevent unbounded memory growth. */
    private static final int MAX_SPLITS_PER_STAGE = SplitPlanningBudget.DEFAULT_MAX_SPLITS;
    private static final int SPLIT_BATCH_SIZE = 1000;

    private SplitFetcher() {}

    /**
     * Fetches raw ConnectorSplit objects from split source descriptors.
     *
     * @throws PangrovaException if any table scan's split enumeration fails
     */
    public static List<ConnectorSplit> fetchRawConnectorSplits(
            List<SplitSourceDescriptor> sources, CatalogRegistry registry,
            Map<String, Long> snapshotOverrides, List<String> queryOutputColumns,
            QuerySession context, FileGranularity fileGranularity) {
        if (sources.isEmpty()) {
            return List.of();
        }
        if (registry == null) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                    "Table split enumeration requires a configured catalog registry");
        }
        Map<List<String>, SplitSourceDescriptor> unique = deduplicateSources(sources);
        List<ConnectorSplit> allSplits = new ArrayList<>();
        long deadlineNanos = deadlineAfter(SPLIT_FETCH_TIMEOUT_MS);
        var stage = new SplitPlanningBudget(deadlineNanos, MAX_SPLITS_PER_STAGE, fileGranularity);
        for (SplitSourceDescriptor desc : unique.values()) {
            requireUninterruptedEnumeration();
            checkSplitFetchLimits(allSplits.size(), deadlineNanos);
            fetchConnectorSplits(desc, registry, snapshotOverrides, queryOutputColumns, context, allSplits,
                    stage);
        }
        requireUninterruptedEnumeration();
        checkSplitFetchLimits(allSplits.size(), deadlineNanos);
        return allSplits;
    }

    /**
     * Converts raw connector splits to the canonical stage representation.
     */
    public static List<QuerySplit> toSplitWithPaths(List<ConnectorSplit> splits) {
        return splits.stream()
                .map(split -> QuerySplit.fromScanMetadata(canonicalSplit(split), extractFilePaths(split),
                        split.estimatedScanBytes(), split.estimatedScanRows()))
                .toList();
    }

    /**
     * Fetches splits with file path metadata from split source descriptors.
     *
     * @throws PangrovaException if any table scan's split enumeration fails
     */
    public static List<QuerySplit> fetchSplitsWithFilePaths(
            List<SplitSourceDescriptor> sources, CatalogRegistry registry,
            Map<String, Long> snapshotOverrides, List<String> queryOutputColumns,
            QuerySession context, FileGranularity fileGranularity) {
        List<ConnectorSplit> allSplits = fetchRawConnectorSplits(sources, registry, snapshotOverrides,
                queryOutputColumns, context, fileGranularity);
        LOG.info("event=split_fetch_summary total_splits={}", allSplits.size());
        return toSplitWithPaths(allSplits);
    }

    /**
     * Extracts file paths from a ConnectorSplit for cache affinity routing.
     */
    private static List<String> extractFilePaths(ConnectorSplit split) {
        List<String> paths = split.dataFilePaths();
        if (!paths.isEmpty()) {
            return paths;
        }
        var props = split.typedSplitProperties();
        String filePath = props.filePath();
        if (filePath != null && !filePath.isEmpty() && !"unknown".equals(filePath)) {
            return List.of(filePath);
        }
        return List.of();
    }

    /**
     * Takes the connector's execution split and attaches any admitted index hints.
     */
    private static org.pangrova.protocol.v2.DataSplit canonicalSplit(ConnectorSplit split) {
        try {
            if (split instanceof IndexEvaluatedSplit evaluated) {
                return IndexEvalHintProtoCodec.withHints(
                        org.pangrova.protocol.source.CanonicalConnectorSplit.from(evaluated.unwrap()),
                        evaluated.evalResult());
            }
            return org.pangrova.protocol.source.CanonicalConnectorSplit.from(split);
        } catch (IllegalArgumentException failure) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                    "Connector did not supply valid canonical DataSplit metadata: " + failure.getMessage(), failure);
        }
    }

    private static void fetchConnectorSplits(
            SplitSourceDescriptor desc, CatalogRegistry registry,
            Map<String, Long> snapshotOverrides, List<String> queryOutputColumns,
            QuerySession context, List<ConnectorSplit> splits, SplitPlanningBudget stage) {
        CatalogHandle catalog = registry.getCatalog(desc.catalogName());
        ConnectorSession session = SessionContextConverter.toConnectorSession(
                context, desc.catalogName(), desc.schemaName());
        var metadata = catalog.getMetadata(session);
        var binding = desc.readBinding().orElseGet(() -> metadata.bindRead(session,
                applySnapshotOverride(resolveTableHandle(catalog, session, desc), desc, snapshotOverrides)));
        Long override = snapshotOverrides.get(org.pangrova.planner.metadata.catalog.PlanningSnapshotSelectors.tableKey(
                desc.catalogName(), desc.schemaName(), desc.tableName()));
        if (override != null && (binding.snapshotId().isEmpty()
                || binding.snapshotId().getAsLong() != override)) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                    "Selected planning snapshot differs from execution selector: " + desc.qualifiedName());
        }
        ConnectorTableHandle tableHandle = binding.handle();

        java.util.Set<String> tableColumnSet = new java.util.LinkedHashSet<>(desc.tableColumns());
        List<String> projectedColumns = queryOutputColumns.stream()
                .filter(tableColumnSet::contains)
                .toList();
        if (!projectedColumns.isEmpty()) {
            metadata.validateColumnAccess(session, tableHandle, projectedColumns);
        }

        if (binding.state() == org.pangrova.connector.api.enums.ReadSnapshotState.EMPTY
                || desc.constraint() == Constraint.NONE) {
            metadata.validateReadBinding(session, binding);
            return;
        }

        var fileGranularity = stage.fileGranularity();
        if (fileGranularity == FileGranularity.BYTE_RANGES) {
            var properties = binding.declaration().metadata().properties();
            if (properties.containsKey(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                    || properties.containsKey(GeneratedColumnMetadataCodec.SHA256_PROPERTY)) {
                fileGranularity = FileGranularity.WHOLE_OBJECT;
            }
        }
        var nativeBinding = NativeReadSplitBinding.prepare(catalog, session);
        ConnectorSplitSource source = catalog.getSplitManager().getBoundSplits(
                ConnectorTransactionHandle.empty(), session, metadata, binding,
                DynamicFilter.ALL, desc.constraint(),
                stage.forSource(stage.remainingSplits() - splits.size(), fileGranularity));
        int previousCount = splits.size();
        drainSource(source, splits, stage.deadlineNanos(), nativeBinding::bind);
        LOG.debug("event=splits_fetched table={} splitCount={}", desc.qualifiedName(), splits.size() - previousCount);
    }

    private static ConnectorTableHandle applySnapshotOverride(
            ConnectorTableHandle handle, SplitSourceDescriptor desc,
            Map<String, Long> overrides) {
        if (overrides.isEmpty()) {
            return handle;
        }
        String fqn = desc.qualifiedName();
        Long snapshotId = overrides.get(org.pangrova.planner.metadata.catalog.PlanningSnapshotSelectors.tableKey(
                desc.catalogName(), desc.schemaName(), desc.tableName()));
        if (snapshotId == null) {
            return handle;
        }
        LOG.info("event=time_travel_snapshot_override table={} snapshotId={}", fqn, snapshotId);
        return handle.withTimeTravelSnapshot(Long.toString(snapshotId));
    }

    private static Map<List<String>, SplitSourceDescriptor> deduplicateSources(
            List<SplitSourceDescriptor> sources) {
        LinkedHashMap<List<String>, SplitSourceDescriptor> unique = new LinkedHashMap<>();
        for (SplitSourceDescriptor desc : sources) {
            var key = List.of(desc.catalogName(), desc.schemaName(), desc.tableName());
            var existing = unique.get(key);
            if (existing != null && (!existing.readBinding().equals(desc.readBinding())
                    || !existing.tableColumns().equals(desc.tableColumns()))) {
                throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                        "Conflicting read bindings for one source table: " + desc.qualifiedName());
            }
            if (!unique.containsKey(key)) {
                unique.put(key, desc);
            } else if (unique.get(key).constraint() != Constraint.EMPTY) {
                // Clear constraint on duplicates to avoid pruning splits
                // that other branches need
                unique.put(key, new SplitSourceDescriptor(
                        desc.catalogName(), desc.schemaName(), desc.tableName(),
                        Constraint.EMPTY, desc.tableColumns(), desc.readBinding()));
            }
        }
        return unique;
    }

    private static ConnectorTableHandle resolveTableHandle(
            CatalogHandle catalog, ConnectorSession session, SplitSourceDescriptor desc) {
        SchemaTableName stn = new SchemaTableName(desc.schemaName(), desc.tableName());
        ConnectorTableHandle handle = catalog.getMetadata(session).getTableHandle(session, stn);
        if (handle == null) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found: " + desc.qualifiedName()
                            + ". Catalog may have returned null for getTableHandle.");
        }
        return handle;
    }

    /** Owns enumeration and the exact source close, including interruption and close failures. */
    static List<ConnectorSplit> drainSource(ConnectorSplitSource source, long timeoutMs) {
        List<ConnectorSplit> splits = new ArrayList<>();
        drainSource(source, splits, deadlineAfter(timeoutMs), java.util.function.UnaryOperator.identity());
        return splits;
    }

    private static long deadlineAfter(long timeoutMillis) {
        return System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }

    static void drainSource(ConnectorSplitSource source, List<ConnectorSplit> splits, long deadlineNanos,
                            java.util.function.UnaryOperator<ConnectorSplit> bind) {
        Throwable primaryFailure = null;
        try {
            while (true) {
                requireUninterruptedEnumeration();
                checkSplitFetchLimits(splits.size(), deadlineNanos);
                boolean finished = source.isFinished();
                requireUninterruptedEnumeration();
                checkSplitFetchLimits(splits.size(), deadlineNanos);
                if (finished) break;

                // Allow an EOF probe at the exact cap without retaining another split.
                int requested = Math.min(SPLIT_BATCH_SIZE, MAX_SPLITS_PER_STAGE - splits.size() + 1);
                List<ConnectorSplit> batch = source.getNextBatch(requested);
                requireUninterruptedEnumeration();
                checkSplitFetchLimits((long) splits.size() + batch.size(), deadlineNanos);
                if (batch.size() > requested) {
                    throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                            "Connector produced " + batch.size() + " splits for a request of " + requested);
                }
                for (ConnectorSplit split : batch) splits.add(bind.apply(split));
            }
        } catch (RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            closeSource(source, primaryFailure);
        }
        requireUninterruptedEnumeration();
        checkSplitFetchLimits(splits.size(), deadlineNanos);
    }

    private static void requireUninterruptedEnumeration() {
        if (Thread.currentThread().isInterrupted()) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                    "Connector split enumeration was interrupted",
                    new InterruptedException("Connector split enumeration was interrupted"));
        }
    }

    private static void closeSource(ConnectorSplitSource source, Throwable primaryFailure) {
        boolean interruptedBeforeClose = Thread.currentThread().isInterrupted();
        try {
            if (primaryFailure == null) {
                source.close();
                return;
            }
            try {
                source.close();
            } catch (RuntimeException | Error closeFailure) {
                if (closeFailure != primaryFailure) primaryFailure.addSuppressed(closeFailure);
            }
        } finally {
            if (interruptedBeforeClose) Thread.currentThread().interrupt();
        }
    }

    /**
     * Checks timeout and split-count limits during split enumeration.
     * Throws instead of silently returning partial results.
     */
    public static void checkSplitFetchLimits(long splitCount, long deadlineNanos) {
        if (System.nanoTime() - deadlineNanos >= 0) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_TIMEOUT,
                    "Split enumeration timed out with " + splitCount + " splits fetched");
        }
        if (splitCount > MAX_SPLITS_PER_STAGE) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED,
                    "Split enumeration exceeded limit: " + splitCount
                            + " splits (max=" + MAX_SPLITS_PER_STAGE + ")");
        }
    }

}
