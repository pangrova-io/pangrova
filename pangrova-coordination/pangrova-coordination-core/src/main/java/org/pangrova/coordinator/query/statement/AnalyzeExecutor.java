package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.query.core.session.SessionTableNames;
import org.pangrova.metadata.core.resolution.ConnectorTableLookup;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.table.core.ownership.TableControlService;
import org.pangrova.table.api.statistics.TableControlStatistics;
import org.pangrova.table.core.statistics.TableControlStatisticsService;
import org.pangrova.parser.api.ast.statement.maintenance.AnalyzeStatement;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Executes {@code ANALYZE TABLE}.
 *
 * <p>Statistics are only collected for controlled tables: an OBSERVED table has no
 * stable identity to bind them to. The result is stored against the stable table
 * ID and the version token observed during collection, so readers can tell
 * whether the row still describes the current table version.</p>
 */
public final class AnalyzeExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AnalyzeExecutor.class);
    private static final String SOURCE = "ANALYZE";

    private final CatalogRegistry catalogs;

    public AnalyzeExecutor(CatalogRegistry catalogs) {
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
    }

    /** Collects, persists, and returns one summary row. */
    public List<TypedRow> execute(AnalyzeStatement stmt,
            QuerySession context) {
        QualifiedTableName ref =
                SessionTableNames.resolveTableRef(stmt.tableName().parts(), context);

        TableControlService control = catalogs.tableControl();
        if (!control.enabled()) {
            throw new org.pangrova.security.api.error.AccessDeniedException(
                    "ANALYZE requires the table control plane: without ownership records "
                            + "no table can be proven safe to analyze");
        }
        var record = control.find(ref.catalog(), ref.schema(), ref.table())
                .orElseThrow(() -> new org.pangrova.security.api.error.AccessDeniedException(
                    "ANALYZE rejected: " + ref
                            + " is OBSERVED; run ALTER TABLE ... ADOPT before ANALYZE"));

        ConnectorSession session =
                SessionContextConverter.toConnectorSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata =
                ConnectorTableLookup.getMetadata(catalogs, ref.catalog(), session);
        ConnectorTableHandle handle =
                ConnectorTableLookup.requireTableHandle(metadata, session, ref);
        control.requireActiveIdentity(record, metadata.getTableIdentity(session, handle), "ANALYZE");

        String before = observe(metadata, session, handle);
        if (before.isEmpty()) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "ANALYZE rejected: the connector cannot observe a version of " + ref
                            + "; collected statistics cannot be bound to a table version");
        }
        TableStatistics statistics = metadata.getTableStatistics(session, handle);
        String after = observeFresh(metadata, session, ref);
        if (after.isEmpty()) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "ANALYZE abandoned: " + ref + " could not be re-resolved after collection");
        }
        if (!before.equals(after)) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "ANALYZE abandoned: the version of " + ref + " changed during collection ("
                            + before + " -> " + after + "); retry when writers are idle");
        }
        String observedToken = after;
        ConnectorTableHandle fresh = ConnectorTableLookup.requireTableHandle(metadata, session, ref);
        control.requireActiveIdentity(record, metadata.getTableIdentity(session, fresh), "ANALYZE");

        TableControlStatisticsService statisticsService = catalogs.tableStatistics();
        TableControlStatistics collected = statisticsService.record(
                record, statistics, SOURCE, observedToken);

        LOG.info("event=analyze_table table={} tableId={} statsVersion={} versionToken={} "
                        + "rowCount={} dataSize={}",
                ref, collected.tableId(), collected.statsVersion(), collected.versionToken(),
                statistics.rowCount(), statistics.dataSize());

        return List.of(TypedRow.builder()
                .putString("table", ref.schema() + "." + ref.table())
                .putLong("stats_version", collected.statsVersion())
                .putString("version_token", collected.versionToken())
                .putLong("row_count", statistics.rowCount())
                .putLong("data_size", statistics.dataSize())
                .putString("source", SOURCE)
                .build());
    }

    private static String observe(ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle handle) {
        Optional<TableVersionToken> version = metadata.observeVersion(session, handle);
        return version.map(TableVersionToken::token).orElse("");
    }

    private static String observeFresh(ConnectorMetadata metadata,
            ConnectorSession session, QualifiedTableName ref) {
        ConnectorTableHandle fresh = metadata.getTableHandle(
                session, new org.pangrova.connector.api.metadata.table.SchemaTableName(
                        ref.schema(), ref.table()));
        if (fresh == null) {
            return "";
        }
        return observe(metadata, session, fresh);
    }
}
