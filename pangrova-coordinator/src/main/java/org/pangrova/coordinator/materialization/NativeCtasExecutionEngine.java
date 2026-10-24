package org.pangrova.coordinator.materialization;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;
import org.pangrova.coordinator.metadata.read.PlanningSnapshotSelectors;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.task.CtasTask;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.materialization.port.ExecutionEngine;
import org.pangrova.materialization.port.ExecutionRequest;
import org.pangrova.materialization.port.ExecutionResult;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.CreateTableAsSelectStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.SnapshotRange;
import org.pangrova.spi.materialization.ChangeSetMode;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.CommitEvidence;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;
import org.pangrova.spi.security.UserIdentity;

/**
 * Native CTAS execution adapter for one coordinator command.
 *
 * <p>The adapter writes only the reserved candidate table under the internal
 * schema, waits until the native commit has completed, and then reads the exact
 * commit evidence captured from the durable write journal. It never exposes
 * rows to the kernel.
 */
public final class NativeCtasExecutionEngine implements ExecutionEngine {

    private final StatementExecutionContext context;
    private final CatalogRegistry catalogs;
    private final CtasTask ctas;
    private final DataSource metadataDataSource;

    /**
     * Creates a native CTAS execution engine for one statement.
     *
     * @param context statement execution context
     * @param catalogs connector catalog registry
     * @param ctas native CTAS task
     * @param metadataDataSource metadata data source
     */
    public NativeCtasExecutionEngine(StatementExecutionContext context, CatalogRegistry catalogs,
            CtasTask ctas, DataSource metadataDataSource) {
        this.context = context;
        this.catalogs = catalogs;
        this.ctas = ctas;
        this.metadataDataSource = metadataDataSource;
    }

    @Override
    public ExecutionResult execute(ExecutionRequest request) {
        Instant now = Instant.now();
        request.capability().requireValidAt(now);
        request.capability().requireCandidate(request.attempt().candidateTable());
        String catalog = request.attempt().candidateTable().catalog();
        StatementExecutionContext scoped = scopedContext(request);
        ensureInternalSchema(catalog);
        RefreshPlan plan = request.plan().orElseThrow(() -> MaterializationException.requestRejected(
                "materialization execution requires a resolved refresh plan"));
        Statement parsed = scoped.compiler().parseToAst(plan.canonicalSql());
        if (!(parsed instanceof Query) && !(parsed instanceof org.pangrova.parser.ast.SetQuery)) {
            throw MaterializationException.invalidDefinition(
                    "materialization definition must be a query: " + request.attempt().object().canonicalName());
        }
        String queryId = request.attempt().fence().executionAnchor();
        if (!queryId.equals(context.queryId())) {
            throw MaterializationException.fenceRejected(
                    "materialization attempt anchor " + queryId + " does not own this query");
        }
        QueryStateMachine state = context.stateMachine();
        state.deferTerminalUntilControlPlaneResult();
        ctas.execute(new CreateTableAsSelectStatement(candidateIdentifier(request), false, parsed), scoped);
        QueryStateMachine.State completion = state.nativeWriteCompletionReady()
                .getNow(QueryStateMachine.State.FAILED);
        if (completion != QueryStateMachine.State.FINISHED) {
            throw new IllegalStateException(
                    "native materialization did not commit before publication: " + completion);
        }
        return new ExecutionResult(readEvidence(request, catalog),
                java.util.OptionalLong.empty(), context.queryId());
    }

    /**
     * Binds the attempt's pinned source vector into the execution session.
     *
     * <p>The session carries only trusted selectors derived from the planned
     * source vector and the resolved refresh plan: exact snapshots for
     * sources read in full or recomputed partition-by-partition and
     * {@code (from, to]} ranges for sources read as an append stream. Client
     * session properties can never reach these maps: the converter strips both
     * selector prefixes before binding.
     */
    private StatementExecutionContext scopedContext(ExecutionRequest request) {
        Map<SourceRef, ChangeSetPlan> changeSets = request.plan()
                .map(RefreshPlan::changeSets).orElse(Map.of());
        Map<String, Long> snapshots = exactSnapshots(request, rangedSources(changeSets));
        Map<String, SnapshotRange> ranges = incrementalRanges(changeSets);
        UserIdentity definer = new UserIdentity(request.definition().definer().name(),
                request.definition().definerGroups(), Map.of(), Instant.now());
        org.pangrova.coordinator.query.session.CoordinatorSessionContext session =
                context.session().withUserIdentity(definer)
                        .withInternalMaterializationExecution();
        if (!snapshots.isEmpty()) {
            session = session.withSourceSnapshots(snapshots);
        }
        if (!ranges.isEmpty()) {
            session = session.withSourceIncrementalRanges(ranges);
        }
        return new StatementExecutionContext(context.stateMachine(), context.queryId(),
                session, context.compiler(), context.resultService());
    }

    private static java.util.Set<SourceRef> rangedSources(
            Map<SourceRef, ChangeSetPlan> changeSets) {
        java.util.Set<SourceRef> ranged = new java.util.LinkedHashSet<>();
        for (Map.Entry<SourceRef, ChangeSetPlan> entry : changeSets.entrySet()) {
            if (entry.getValue().mode() != ChangeSetMode.PARTITION_REPLACE) {
                ranged.add(entry.getKey());
            }
        }
        return ranged;
    }

    private Map<String, Long> exactSnapshots(ExecutionRequest request,
            java.util.Set<SourceRef> rangedSources) {
        Map<String, Long> snapshots = new LinkedHashMap<>();
        for (Map.Entry<SourceRef, SourceSnapshotRef> entry
                : request.attempt().sourceVector().snapshots().entrySet()) {
            SourceRef source = entry.getKey();
            if (rangedSources.contains(source)) {
                continue;
            }
            if (entry.getValue().isEmptySource()) {
                // An empty source has no snapshot to select; the full scan sees the table as-is.
                continue;
            }
            long snapshot = snapshotId(source, entry.getValue().snapshotToken());
            requireOwningProvider(source);
            String key = PlanningSnapshotSelectors.tableKey(
                    source.catalog(), source.schema(), source.table());
            Long previous = snapshots.putIfAbsent(key, snapshot);
            if (previous != null && previous != snapshot) {
                throw MaterializationException.capabilityUnavailable(
                        "conflicting planned snapshots for " + source.canonicalName());
            }
        }
        return snapshots;
    }

    private Map<String, SnapshotRange> incrementalRanges(Map<SourceRef, ChangeSetPlan> changeSets) {
        Map<String, SnapshotRange> ranges = new LinkedHashMap<>();
        for (Map.Entry<SourceRef, ChangeSetPlan> entry : changeSets.entrySet()) {
            SourceRef source = entry.getKey();
            ChangeSetPlan changeSet = entry.getValue();
            if (!changeSet.source().equals(source)) {
                throw MaterializationException.capabilityUnavailable(
                        "change set does not belong to " + source.canonicalName());
            }
            if (changeSet.mode() == ChangeSetMode.PARTITION_REPLACE) {
                // Partition replacement recomputes whole partitions from the
                // target snapshot; an append range would only expose the files
                // touched by the interval.
                continue;
            }
            requireOwningProvider(source);
            long from = snapshotId(source, changeSet.fromSnapshot().snapshotToken());
            long to = snapshotId(source, changeSet.toSnapshot().snapshotToken());
            SnapshotRange range;
            try {
                range = new SnapshotRange(from, to);
            } catch (IllegalArgumentException invalid) {
                throw MaterializationException.capabilityUnavailable(
                        "source " + source.canonicalName()
                                + " has an invalid snapshot interval: " + from + ".." + to, invalid);
            }
            String key = PlanningSnapshotSelectors.tableKey(
                    source.catalog(), source.schema(), source.table());
            SnapshotRange previous = ranges.putIfAbsent(key, range);
            if (previous != null && !previous.equals(range)) {
                throw MaterializationException.capabilityUnavailable(
                        "conflicting planned ranges for " + source.canonicalName());
            }
        }
        return ranges;
    }

    private long snapshotId(SourceRef source, String token) {
        long snapshot;
        try {
            snapshot = Long.parseLong(token);
        } catch (NumberFormatException invalid) {
            throw MaterializationException.capabilityUnavailable(
                    "source " + source.canonicalName()
                            + " has no exact numeric snapshot for CTAS: " + token);
        }
        if (snapshot <= 0) {
            throw MaterializationException.capabilityUnavailable(
                    "source " + source.canonicalName()
                            + " has no positive snapshot for CTAS: " + snapshot);
        }
        return snapshot;
    }

    private void requireOwningProvider(SourceRef source) {
        String providerId = catalogs.getCatalog(source.catalog()).getDescriptor().provider()
                .toLowerCase(Locale.ROOT);
        if (!providerId.equals(source.connectorId())) {
            throw MaterializationException.capabilityUnavailable(
                    "source " + source.canonicalName() + " connector " + source.connectorId()
                            + " does not own catalog " + source.catalog() + " (" + providerId + ")");
        }
    }

    private void ensureInternalSchema(String catalog) {
        ConnectorSession session = org.pangrova.coordinator.query.statement.TableResolver
                .buildSession(context.session(), catalog, InternalNamespace.SCHEMA_NAME);
        var metadata = catalogs.getCatalog(catalog).getMetadata(session);
        if (!metadata.listSchemaNames(session).contains(InternalNamespace.SCHEMA_NAME)) {
            metadata.createSchema(session, InternalNamespace.SCHEMA_NAME);
        }
    }

    private static Identifier candidateIdentifier(ExecutionRequest request) {
        return new Identifier(List.of(
                request.attempt().candidateTable().catalog(),
                request.attempt().candidateTable().schema(),
                request.attempt().candidateTable().table()));
    }

    private CommitEvidence readEvidence(ExecutionRequest request, String catalog) {
        String candidate = request.attempt().candidateTable().catalog() + '.'
                + request.attempt().candidateTable().schema() + '.'
                + request.attempt().candidateTable().table();
        try (Connection connection = metadataDataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT catalog_name, table_name, commit_fingerprint, manifest_sha256, committed_at "
                             + "FROM materialization.native_commit_capture WHERE query_id=?")) {
            statement.setString(1, context.queryId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw MaterializationException.evidenceMismatch(
                            "native commit capture is missing for query " + context.queryId());
                }
                String committedCandidate = result.getString("catalog_name") + '.'
                        + result.getString("table_name");
                if (!candidate.equals(committedCandidate)) {
                    throw MaterializationException.evidenceMismatch(
                            "native commit targeted " + committedCandidate
                                    + " instead of the reserved candidate " + candidate);
                }
                return new CommitEvidence(
                        catalogProviderId(catalog),
                        result.getString("commit_fingerprint"),
                        HexFormat.of().formatHex(result.getBytes("manifest_sha256")),
                        "native-commit-v1",
                        result.getTimestamp("committed_at").toInstant());
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("native commit capture read failed", failure);
        }
    }

    private String catalogProviderId(String catalog) {
        return catalogs.getCatalog(catalog).getDescriptor().provider().toLowerCase(Locale.ROOT);
    }
}
