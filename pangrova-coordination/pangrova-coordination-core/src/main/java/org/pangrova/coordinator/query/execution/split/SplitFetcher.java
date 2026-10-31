package org.pangrova.coordinator.query.execution.split;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.protocol.index.IndexEvalHintProtoCodec;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;
import org.pangrova.connector.spi.metadata.Constraint;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.pangrova.indexing.api.eval.IndexEvaluatedSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches splits from connector catalogs based on extracted table scan descriptors.
 *
 * <p>This class is Calcite-free: all plan analysis and predicate extraction is
 * performed by {@link org.pangrova.planner.PlanSplitSourceExtractor} in the planner
 * module. SplitFetcher works exclusively with the Calcite-free
 * {@link SplitSourceDescriptor} representation.</p>
 */
public final class SplitFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(SplitFetcher.class);

    /** Maximum time to spend fetching splits per table scan. */
    private static final long SPLIT_FETCH_TIMEOUT_MS = 30_000;

    /** Maximum number of splits per stage to prevent unbounded memory growth. */
    private static final int MAX_SPLITS_PER_STAGE = 100_000;

    private SplitFetcher() {}

    /**
     * Serialized split data paired with file paths for cache affinity routing.
     */
    public record SplitWithPaths(String serializedData, List<String> filePaths) {
        public SplitWithPaths {
            filePaths = filePaths != null ? List.copyOf(filePaths) : List.of();
        }
    }

    /**
     * Converts fetched splits into assignment records for pull-mode registration.
     * Centralizes the SplitWithPaths → SplitAssignment mapping to avoid duplication.
     */
    public static List<SplitAssignment> toAssignments(List<SplitWithPaths> splits) {
        if (splits.isEmpty()) {
            return List.of();
        }
        long now = System.currentTimeMillis();
        List<SplitAssignment> assignments = new ArrayList<>(splits.size());
        for (int i = 0; i < splits.size(); i++) {
            SplitWithPaths swp = splits.get(i);
            assignments.add(new SplitAssignment(i, swp.serializedData(),
                    List.of(), swp.filePaths(), now));
        }
        return assignments;
    }

    /**
     * Fetches raw ConnectorSplit objects for a PlannedQuery.
     */
    public static List<ConnectorSplit> fetchRawConnectorSplits(
            PlannedQuery plannedQuery, CatalogRegistry registry,
            Map<String, Long> snapshotOverrides, CoordinatorSessionContext context) {
        return fetchRawConnectorSplits(
                plannedQuery.splitSources(), registry, snapshotOverrides,
                plannedQuery.queryOutputColumns(), context);
    }

    /**
     * Fetches raw ConnectorSplit objects from split source descriptors.
     *
     * @throws PangrovaException if any table scan's split enumeration fails
     */
    public static List<ConnectorSplit> fetchRawConnectorSplits(
            List<SplitSourceDescriptor> sources, CatalogRegistry registry,
            Map<String, Long> snapshotOverrides, List<String> queryOutputColumns,
            CoordinatorSessionContext context) {
        if (registry == null || sources.isEmpty()) {
            return List.of();
        }
        Map<List<String>, SplitSourceDescriptor> unique = deduplicateSources(sources);
        List<ConnectorSplit> allSplits = new ArrayList<>();
        for (SplitSourceDescriptor desc : unique.values()) {
            allSplits.addAll(fetchConnectorSplits(desc, registry, snapshotOverrides, queryOutputColumns, context));
        }
        return allSplits;
    }

    /**
     * Converts raw ConnectorSplits to SplitWithPaths (serialized form).
     */
    public static List<SplitWithPaths> toSplitWithPaths(List<ConnectorSplit> splits) {
        return splits.stream()
                .map(split -> new SplitWithPaths(serializeSplit(split), extractFilePaths(split)))
                .toList();
    }

    /**
     * Fetches splits with file path metadata for a PlannedQuery.
     *
     * @param snapshotOverrides table FQN (catalog.schema.table) → snapshot ID
     * @throws PangrovaException if any table scan's split enumeration fails
     */
    public static List<SplitWithPaths> fetchSplitsWithFilePaths(
            PlannedQuery plannedQuery, CatalogRegistry registry,
            Map<String, Long> snapshotOverrides, CoordinatorSessionContext context) {
        List<ConnectorSplit> allSplits = fetchRawConnectorSplits(plannedQuery, registry, snapshotOverrides, context);
        LOG.info("event=split_fetch_summary total_splits={}", allSplits.size());
        return toSplitWithPaths(allSplits);
    }

    /**
     * Fetches splits with file path metadata from split source descriptors.
     *
     * @throws PangrovaException if any table scan's split enumeration fails
     */
    public static List<SplitWithPaths> fetchSplitsWithFilePaths(
            List<SplitSourceDescriptor> sources, CatalogRegistry registry,
            Map<String, Long> snapshotOverrides, List<String> queryOutputColumns,
            CoordinatorSessionContext context) {
        List<ConnectorSplit> allSplits = fetchRawConnectorSplits(sources, registry, snapshotOverrides,
                queryOutputColumns, context);
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
     * Serializes a ConnectorSplit to Base64-encoded proto bytes.
     */
    private static String serializeSplit(ConnectorSplit split) {
        byte[] protoBytes;
        if (split instanceof IndexEvaluatedSplit evaluated
                && evaluated.evalResult().hasEvalHints()) {
            protoBytes = IndexEvalHintProtoCodec.serializeWithHints(evaluated);
        } else {
            protoBytes = split.toSplitInfoProto();
        }
        if (protoBytes.length == 0) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                    "ConnectorSplit.toSplitInfoProto() returned empty bytes; "
                            + "connector must implement proto serialization");
        }
        return java.util.Base64.getEncoder().encodeToString(protoBytes);
    }

    private static List<ConnectorSplit> fetchConnectorSplits(
            SplitSourceDescriptor desc, CatalogRegistry registry,
            Map<String, Long> snapshotOverrides, List<String> queryOutputColumns,
            CoordinatorSessionContext context) {
        CatalogHandle catalog = registry.getCatalog(desc.catalogName());
        ConnectorSession session = SessionContextConverter.toConnectorSession(
                context, desc.catalogName(), desc.schemaName());
        var metadata = catalog.getMetadata(session);
        var binding = desc.readBinding().orElseGet(() -> metadata.bindRead(session,
                applySnapshotOverride(resolveTableHandle(catalog, session, desc), desc, snapshotOverrides)));
        Long override = snapshotOverrides.get(org.pangrova.coordinator.metadata.read.PlanningSnapshotSelectors.tableKey(
                desc.catalogName(), desc.schemaName(), desc.tableName()));
        if (override != null && (binding.snapshotId().isEmpty()
                || binding.snapshotId().getAsLong() != override)) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_FAILED,
                    "Selected planning snapshot differs from execution selector: " + desc.qualifiedName());
        }
        metadata.validateReadBinding(session, binding);
        ConnectorTableHandle tableHandle = binding.handle();

        java.util.Set<String> tableColumnSet = new java.util.LinkedHashSet<>(desc.tableColumns());
        List<String> projectedColumns = queryOutputColumns.stream()
                .filter(tableColumnSet::contains)
                .toList();
        if (!projectedColumns.isEmpty()) {
            metadata.validateColumnAccess(session, tableHandle, projectedColumns);
        }

        if (binding.state() == org.pangrova.connector.spi.metadata.table.ConnectorReadBinding.SnapshotState.EMPTY) {
            return List.of();
        }

        Constraint constraint = desc.constraint();
        List<ConnectorSplit> splits = drainSplits(catalog, session, tableHandle, constraint);
        LOG.debug("event=splits_fetched table={} splitCount={}", desc.qualifiedName(), splits.size());
        return splits.stream().map(split -> NativeReadSplitBinding.bind(split, catalog, session)).toList();
    }

    private static ConnectorTableHandle applySnapshotOverride(
            ConnectorTableHandle handle, SplitSourceDescriptor desc,
            Map<String, Long> overrides) {
        if (overrides.isEmpty()) {
            return handle;
        }
        String fqn = desc.qualifiedName();
        Long snapshotId = overrides.get(org.pangrova.coordinator.metadata.read.PlanningSnapshotSelectors.tableKey(
                desc.catalogName(), desc.schemaName(), desc.tableName()));
        if (snapshotId == null) {
            return handle;
        }
        LOG.info("event=time_travel_snapshot_override table={} snapshotId={}", fqn, snapshotId);
        return handle.withTimeTravelSnapshot(snapshotId);
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

    private static List<ConnectorSplit> drainSplits(
            CatalogHandle catalog, ConnectorSession session,
            ConnectorTableHandle tableHandle, Constraint constraint) {
        return drainSplits(catalog, session, tableHandle, constraint, SPLIT_FETCH_TIMEOUT_MS);
    }

    private static List<ConnectorSplit> drainSplits(
            CatalogHandle catalog, ConnectorSession session,
            ConnectorTableHandle tableHandle, Constraint constraint,
            long timeoutMs) {
        ConnectorSplitSource source = catalog.getSplitManager().getSplits(
                ConnectorTransactionHandle.empty(), session, tableHandle,
                DynamicFilter.ALL, constraint);
        return drainSource(source, timeoutMs);
    }

    /** Owns enumeration and the exact source close, including interruption and close failures. */
    static List<ConnectorSplit> drainSource(ConnectorSplitSource source, long timeoutMs) {
        List<ConnectorSplit> splits = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        Throwable primaryFailure = null;
        try {
            requireUninterruptedEnumeration();
            while (!source.isFinished()) {
                requireUninterruptedEnumeration();
                checkSplitFetchLimits(splits, deadline);
                splits.addAll(source.getNextBatch(1000));
                requireUninterruptedEnumeration();
            }
            requireUninterruptedEnumeration();
        } catch (RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            closeSource(source, primaryFailure);
        }
        requireUninterruptedEnumeration();
        return splits;
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
    public static void checkSplitFetchLimits(List<ConnectorSplit> splits, long deadline) {
        if (System.currentTimeMillis() > deadline) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_TIMEOUT,
                    "Split enumeration timed out after " + SPLIT_FETCH_TIMEOUT_MS
                            + "ms with " + splits.size() + " splits fetched");
        }
        if (splits.size() >= MAX_SPLITS_PER_STAGE) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_LIMIT_REACHED,
                    "Split enumeration exceeded limit: " + splits.size()
                            + " splits (max=" + MAX_SPLITS_PER_STAGE + ")");
        }
    }

}
