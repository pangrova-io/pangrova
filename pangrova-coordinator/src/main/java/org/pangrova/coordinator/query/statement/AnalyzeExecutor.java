package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.control.TableControlService;
import org.pangrova.metadata.control.TableControlStatistics;
import org.pangrova.metadata.control.TableControlStatisticsService;
import org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
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
            CoordinatorSessionContext context) {
        QualifiedTableName ref =
                TableResolver.resolveTableRef(stmt.tableName().parts(), context);

        TableControlService control = catalogs.tableControl();
        if (!control.enabled()) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "ANALYZE requires the table control plane: without ownership records "
                            + "no table can be proven safe to analyze");
        }
        if (control.find(ref.catalog(), ref.schema(), ref.table()).isEmpty()) {
            throw new org.pangrova.spi.security.AccessDeniedException(
                    "ANALYZE rejected: " + ref
                            + " is OBSERVED; run ALTER TABLE ... ADOPT before ANALYZE");
        }

        ConnectorSession session =
                TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata =
                TableResolver.getMetadata(catalogs, ref.catalog(), session);
        ConnectorTableHandle handle =
                TableResolver.requireTableHandle(metadata, session, ref);

        String before = observe(metadata, session, handle);
        TableStatistics statistics = metadata.getTableStatistics(session, handle);
        String after = observeFresh(metadata, session, ref);
        if (before.isEmpty() && after.isEmpty()) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.CONNECTOR_UNSUPPORTED_OPERATION,
                    "ANALYZE rejected: the connector cannot observe a version of " + ref
                            + "; collected statistics cannot be bound to a table version");
        }
        if (after.isEmpty()) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "ANALYZE abandoned: " + ref + " could not be re-resolved after collection");
        }
        if (!before.isEmpty() && !before.equals(after)) {
            throw new org.pangrova.common.exception.PangrovaException(
                    org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "ANALYZE abandoned: the version of " + ref + " changed during collection ("
                            + before + " -> " + after + "); retry when writers are idle");
        }
        String observedToken = after;

        TableControlStatisticsService statisticsService = catalogs.tableStatistics();
        TableControlStatistics collected = statisticsService.record(
                ref.catalog(), ref.schema(), ref.table(), statistics, SOURCE, observedToken);

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
                session, new org.pangrova.spi.connector.metadata.table.SchemaTableName(
                        ref.schema(), ref.table()));
        if (fresh == null) {
            return "";
        }
        return observe(metadata, session, fresh);
    }
}
