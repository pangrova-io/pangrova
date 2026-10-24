package org.pangrova.coordinator.materialization;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.materialization.domain.Binding;
import org.pangrova.materialization.domain.ColumnDefinition;
import org.pangrova.materialization.domain.Dependency;
import org.pangrova.materialization.domain.DependencyKind;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.OutputSchema;
import org.pangrova.materialization.domain.PartitionAlignment;
import org.pangrova.materialization.domain.PolicySnapshot;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.DefinitionPlanner;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.lineage.ColumnDerivation;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;
import org.pangrova.spi.materialization.ChangeSetMode;
import org.pangrova.spi.materialization.MaterializationCapabilities;
import org.pangrova.spi.materialization.MaterializationSourceException;
import org.pangrova.spi.materialization.SourceProvider;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;

/**
 * Production definition planner for the materialization kernel.
 *
 * <p>The adapter compiles the definition with the real Calcite planner, pins
 * every versioned source snapshot, derives the output schema from the planned
 * row type, and resolves durable upstream materializations into dependency
 * edges. A source without a pinned snapshot is rejected: the kernel has no
 * unpinned production path.
 */
public final class CoordinatorMaterializationPlanner implements DefinitionPlanner {

    private static final long VECTOR_VERSION = 1;

    private final SqlCompiler compiler;
    private final CoordinatorSessionContext session;
    private final CatalogRegistry catalogs;
    private final MaterializationCatalog materializations;
    private final TenantId tenant;
    private final AccessControl accessControl;

    /**
     * Creates the definition planner for one kernel session.
     *
     * @param compiler session compiler snapshot
     * @param session authenticated session
     * @param catalogs connector catalog registry
     * @param materializations published-materialization catalog
     * @param tenant authenticated tenant
     * @param accessControl access-control policy source, or null when disabled
     */
    public CoordinatorMaterializationPlanner(SqlCompiler compiler, CoordinatorSessionContext session,
            CatalogRegistry catalogs, MaterializationCatalog materializations, TenantId tenant,
            AccessControl accessControl) {
        this.compiler = java.util.Objects.requireNonNull(compiler, "compiler");
        this.session = java.util.Objects.requireNonNull(session, "session");
        this.catalogs = java.util.Objects.requireNonNull(catalogs, "catalogs");
        this.materializations = java.util.Objects.requireNonNull(materializations, "materializations");
        this.tenant = java.util.Objects.requireNonNull(tenant, "tenant");
        this.accessControl = accessControl;
    }

    @Override
    public PlannedDefinition plan(PlanningRequest request) {
        ConnectorSession connectorSession = SessionContextConverter.toConnectorSession(
                planningSession(request.sessionProperties()));
        PlannedQuery planned = compiler.forSession(connectorSession)
                .compileDefinitionQuery(request.definitionSql());
        OutputSchema schema = outputSchema(planned);
        Map<SourceRef, SourceSnapshotRef> snapshots = new LinkedHashMap<>();
        Set<SourceRef> fullOnlySources = new LinkedHashSet<>();
        pinSources(planned, request, snapshots, fullOnlySources);
        String planDigest = planned.effectivePlanDigest();
        PolicySnapshot policies = new PolicySnapshot(
                policyFingerprints(snapshots.keySet(), request, planned));
        PartitionAlignment alignment = partitionAlignment(planned);
        return new PlannedDefinition(
                new Binding(request.definitionSql(), planDigest),
                schema,
                new SourceVector(snapshots, VECTOR_VERSION),
                dependencies(planned, request),
                policies,
                supportedRefreshKinds(planned, snapshots.keySet(), fullOnlySources, alignment),
                alignment);
    }

    /**
     * Maps the directly derived output columns that a partition rewrite can
     * filter on. The connector reports partition columns by source name, so
     * only a direct projection of that source column can carry the predicate
     * into the definition result.
     */
    private static PartitionAlignment partitionAlignment(PlannedQuery planned) {
        PartitionAlignment.Shape shape = planned.isAppendOnlyScanCompatible()
                ? PartitionAlignment.Shape.SCAN
                : planned.isPartitionLocalAggregate()
                        ? PartitionAlignment.Shape.AGGREGATE
                        : PartitionAlignment.Shape.UNSUPPORTED;
        if (shape == PartitionAlignment.Shape.UNSUPPORTED) {
            return PartitionAlignment.NONE;
        }
        boolean groupingKey = shape == PartitionAlignment.Shape.AGGREGATE;
        Map<String, PartitionAlignment.Column> columns = new LinkedHashMap<>();
        for (ColumnDerivation derivation : planned.columnLineage()) {
            if (derivation.expression() != ColumnDerivation.DerivationType.DIRECT
                    || derivation.sourceColumns().size() != 1) {
                continue;
            }
            String sourceColumn = derivation.sourceColumns().get(0).column();
            columns.putIfAbsent(sourceColumn.toLowerCase(Locale.ROOT),
                    new PartitionAlignment.Column(
                            sourceColumn, derivation.outputColumn(), groupingKey));
        }
        return new PartitionAlignment(shape, List.copyOf(columns.values()));
    }

    /**
     * Returns the refresh kinds this exact plan and its connectors can serve.
     *
     * <p>Append-only refresh requires a single-table projection/filter plan.
     * Partition replacement additionally accepts a single-table grouping
     * aggregate. Both require a connector that can pin snapshots, enumerate
     * the interval, and prove the corresponding change set. Every other shape
     * is full-only; there is no heuristic downgrade at refresh time.
     */
    private Set<RefreshKind> supportedRefreshKinds(PlannedQuery planned, Set<SourceRef> sources,
            Set<SourceRef> fullOnlySources, PartitionAlignment alignment) {
        Set<RefreshKind> kinds = new LinkedHashSet<>();
        kinds.add(RefreshKind.Full.INSTANCE);
        // An upstream materialization is read through its current physical
        // table, and every publication replaces that table. A change proof can
        // therefore never span the base generation and the newly pinned table,
        // so a dependent definition is full-only even when the connector could
        // prove append-only intervals on a stable source.
        boolean generatedSource = sources.stream()
                .anyMatch(source -> InternalNamespace.SCHEMA_NAME.equals(source.schema()));
        if (sources.size() != 1 || !fullOnlySources.isEmpty() || generatedSource
                || !planned.isResultCacheSafe()) {
            return Set.copyOf(kinds);
        }
        SourceRef source = sources.iterator().next();
        Optional<SourceProvider> provider =
                catalogs.getCatalog(source.catalog()).getMaterializationSourceProvider();
        if (provider.isEmpty() || !provider.get().connectorId().equals(source.connectorId())) {
            return Set.copyOf(kinds);
        }
        MaterializationCapabilities capabilities = provider.get().capabilities(source);
        capabilities.requireCompatibleVersion();
        if (capabilities.supports(ChangeSetMode.APPEND_ONLY)
                && planned.isAppendOnlyScanCompatible()) {
            kinds.add(RefreshKind.IncrementalAppendOnly.INSTANCE);
        }
        if (capabilities.supports(ChangeSetMode.PARTITION_REPLACE)
                && partitionReplaceAligned(alignment, provider.get().partitionColumns(source))) {
            kinds.add(RefreshKind.IncrementalPartitionReplace.INSTANCE);
        }
        return Set.copyOf(kinds);
    }

    private static boolean partitionReplaceAligned(
            PartitionAlignment alignment, List<String> partitionColumns) {
        if (alignment.shape() == PartitionAlignment.Shape.UNSUPPORTED
                || partitionColumns.isEmpty()) {
            return false;
        }
        for (String column : partitionColumns) {
            Optional<PartitionAlignment.Column> mapped = alignment.columnFor(column);
            if (mapped.isEmpty()) {
                return false;
            }
            if (alignment.shape() == PartitionAlignment.Shape.AGGREGATE
                    && !mapped.get().groupingKey()) {
                return false;
            }
        }
        return true;
    }

    private CoordinatorSessionContext planningSession(Map<String, String> requestProperties) {
        Map<String, String> properties = new java.util.HashMap<>(session.sessionProperties());
        properties.putAll(requestProperties);
        return session.withSessionProperties(properties);
    }

    private OutputSchema outputSchema(PlannedQuery planned) {
        List<String> names = planned.queryOutputColumns();
        List<String> types = planned.queryOutputColumnTypeNames();
        if (names.isEmpty() || names.size() != types.size()) {
            throw MaterializationException.invalidDefinition(
                    "definition output schema must expose matching names and types");
        }
        return new OutputSchema(IntStream.range(0, names.size())
                .mapToObj(index -> new ColumnDefinition(names.get(index), types.get(index), true, index))
                .toList());
    }

    private void pinSources(PlannedQuery planned, PlanningRequest request,
            Map<SourceRef, SourceSnapshotRef> snapshots, Set<SourceRef> fullOnlySources) {
        for (SplitSourceDescriptor descriptor : planned.splitSources()) {
            ConnectorReadBinding binding = descriptor.readBinding().orElseThrow(() ->
                    MaterializationException.capabilityUnavailable(
                            "source " + descriptor.qualifiedName()
                                    + " cannot be pinned for materialization"));
            boolean empty = binding.state() == ConnectorReadBinding.SnapshotState.EMPTY;
            if (binding.state() != ConnectorReadBinding.SnapshotState.SNAPSHOT && !empty) {
                throw MaterializationException.capabilityUnavailable(
                        "source " + descriptor.qualifiedName() + " in state " + binding.state()
                                + " has no exact snapshot and cannot be materialized");
            }
            String connectorId = connectorId(descriptor.catalogName());
            SourceRef source = new SourceRef(connectorId, descriptor.catalogName(),
                    descriptor.schemaName(), descriptor.tableName());
            SourceProvider provider = provider(source, connectorId);
            String policy = policyFingerprint(source, request, planned);
            SourceSnapshotRef described;
            try {
                provider.capabilities(source).requireCompatibleVersion();
                described = empty
                        ? provider.describeEmpty(source, tenant.value(),
                                binding.sourceIdentity(), policy)
                        : provider.describe(source, tenant.value(), snapshotToken(binding), policy);
            } catch (MaterializationSourceException failure) {
                throw MaterializationException.capabilityUnavailable(
                        "cannot describe source " + source.canonicalName()
                                + ": " + failure.reason(), failure);
            }
            if (!connectorId.equals(described.connectorId()) || empty != described.isEmptySource()) {
                throw MaterializationException.capabilityUnavailable(
                        "connector " + connectorId + " returned an inconsistent snapshot identity for "
                                + source.canonicalName());
            }
            snapshots.put(source, described);
            if (empty) {
                fullOnlySources.add(source);
            }
        }
    }

    private SourceProvider provider(SourceRef source, String connectorId) {
        SourceProvider provider = catalogs.getCatalog(source.catalog())
                .getMaterializationSourceProvider()
                .orElseThrow(() -> MaterializationException.capabilityUnavailable(
                        "catalog " + source.catalog()
                                + " cannot pin materialization source snapshots"));
        if (!provider.connectorId().equals(connectorId)) {
            throw MaterializationException.requestRejected(
                    "source connector " + connectorId + " does not match catalog "
                            + source.catalog());
        }
        return provider;
    }

    private Set<Dependency> dependencies(PlannedQuery planned, PlanningRequest request) {
        Set<Dependency> dependencies = new LinkedHashSet<>();
        planned.physicalToLogicalDynamicTables().values().forEach(logical -> {
            MaterializationId upstream = new MaterializationId(tenant,
                    QualifiedName.parse(logical.toString()));
            if (upstream.equals(request.object())) {
                return;
            }
            materializations.findPublished(upstream).ifPresent(published ->
                    dependencies.add(new Dependency(upstream, request.object(),
                            request.definitionRevision(), DependencyKind.DIRECT)));
        });
        return dependencies;
    }

    private Map<SourceRef, String> policyFingerprints(
            Set<SourceRef> sources, PlanningRequest request, PlannedQuery planned) {
        Map<SourceRef, String> fingerprints = new LinkedHashMap<>();
        for (SourceRef source : sources) {
            fingerprints.put(source, policyFingerprint(source, request, planned));
        }
        return fingerprints;
    }

    /**
     * Captures the row filter and column masks that the definer would see.
     *
     * <p>The identity is rebuilt from the durable definer principal instead of
     * the current session, so a manual refresh by another principal computes
     * the same fingerprint as the create command that recorded it.
     */
    private String policyFingerprint(SourceRef source, PlanningRequest request,
            PlannedQuery planned) {
        StringBuilder state = new StringBuilder("policy:")
                .append(request.definer().name()).append(':')
                .append(source.canonicalName()).append(':')
                .append(planned.effectivePlanDigest());
        if (accessControl != null) {
            UserIdentity definer = new UserIdentity(request.definer().name(),
                    request.definerGroups(), Map.of(), java.time.Instant.now());
            state.append(":row=").append(accessControl.getRowFilter(definer,
                    source.catalog(), source.schema(), source.table()).orElse(""));
            for (String column : referencedColumns(planned, source)) {
                state.append(":mask:").append(column).append('=')
                        .append(accessControl.getColumnMask(definer, source.catalog(),
                                source.schema(), source.table(), column).orElse(""));
            }
        }
        return sha256(state.toString());
    }

    private static List<String> referencedColumns(PlannedQuery planned, SourceRef source) {
        java.util.TreeSet<String> columns = new java.util.TreeSet<>(
                String.CASE_INSENSITIVE_ORDER);
        for (ColumnDerivation derivation : planned.columnLineage()) {
            for (ColumnDerivation.SourceColumn column : derivation.sourceColumns()) {
                String table = column.table();
                if (table.equals(source.table())
                        || table.endsWith('.' + source.table())) {
                    columns.add(column.column());
                }
            }
        }
        return List.copyOf(columns);
    }

    private static String snapshotToken(ConnectorReadBinding binding) {
        return binding.snapshotId().isPresent()
                ? Long.toString(binding.snapshotId().getAsLong())
                : binding.sourceIdentity();
    }

    private String connectorId(String catalogName) {
        return catalogs.getCatalog(catalogName).getDescriptor().provider()
                .toLowerCase(Locale.ROOT);
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", failure);
        }
    }
}
