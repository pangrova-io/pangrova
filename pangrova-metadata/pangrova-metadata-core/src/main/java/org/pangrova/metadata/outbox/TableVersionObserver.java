package org.pangrova.metadata.outbox;

import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.table.api.observation.ObservationOutcome;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.identity.TableRef;
import org.pangrova.table.core.ownership.TableControlService;
import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.service.DefaultConnectorSession;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Reconciles controlled tables against what the connector currently reports.
 *
 * <p>Only LINKED and MANAGED tables are observed. OBSERVED tables are a live
 * catalog view with no persisted identity, so resolution validates them directly
 * instead of feeding this loop.</p>
 *
 * <p>A catalog that cannot be reached aborts the whole catalog observation. A
 * discovery failure is not evidence that its tables were dropped, so it must
 * never mark records stale.</p>
 */
public final class TableVersionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(TableVersionObserver.class);
    private static final String SOURCE = "PERIODIC";
    private static final String SYSTEM_USER = "system";
    private static final String OBSERVATION_QUERY_ID = "table-version-observation";

    private final CatalogRegistry catalogRegistry;
    private final MetadataEventDispatcher eventDispatcher;
    private final Clock clock;

    public TableVersionObserver(CatalogRegistry catalogRegistry,
            MetadataEventDispatcher eventDispatcher) {
        this(catalogRegistry, eventDispatcher, Clock.systemUTC());
    }

    public TableVersionObserver(CatalogRegistry catalogRegistry,
            MetadataEventDispatcher eventDispatcher, Clock clock) {
        this.catalogRegistry = catalogRegistry;
        this.eventDispatcher = eventDispatcher;
        this.clock = clock;
    }

    /** Observes every registered catalog, isolating per-catalog failures. */
    public List<TableVersionObservationReport> observeAll() {
        List<TableVersionObservationReport> reports = new ArrayList<>();
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            try {
                reports.add(observeCatalog(catalogName));
            } catch (RuntimeException failure) {
                LOG.warn("event=table_version_observation_catalog_failed catalog={} error={}",
                        catalogName, failure.toString());
            }
        }
        return reports;
    }

    /**
     * Observes every live control record in one catalog. Returns an empty report
     * without touching the connector when the control plane is disabled or the
     * catalog has no controlled tables.
     */
    public TableVersionObservationReport observeCatalog(String catalogName) {
        TableControlService control = catalogRegistry.tableControl();
        if (!control.enabled()) {
            return TableVersionObservationReport.empty(catalogName);
        }
        List<TableControlRecord> records = control.listLive(catalogName);
        if (records.isEmpty()) {
            return TableVersionObservationReport.empty(catalogName);
        }

        CatalogHandle catalog;
        ConnectorMetadata metadata;
        try {
            catalog = catalogRegistry.getCatalog(catalogName);
            metadata = catalog.getMetadata(systemSession(catalogName, null));
        } catch (RuntimeException unavailable) {
            // An unreachable catalog is not evidence that its tables disappeared.
            return TableVersionObservationReport.unavailable(catalogName, unavailable.toString());
        }
        if (metadata == null) {
            return TableVersionObservationReport.unavailable(
                    catalogName, "catalog published no metadata provider");
        }

        ReportBuilder report = new ReportBuilder(catalogName);
        for (TableControlRecord record : records) {
            try {
                report.record(observeOne(control, metadata, catalogName, record));
            } catch (RuntimeException failure) {
                report.error(record.qualifiedName() + ": " + failure);
                LOG.warn("event=table_version_observation_failed table={} error={}",
                        record.qualifiedName(), failure.toString());
            }
        }
        return report.build();
    }

    private ObservationOutcome observeOne(TableControlService control,
            ConnectorMetadata metadata, String catalogName, TableControlRecord record) {
        Instant now = clock.instant();
        ConnectorSession session = systemSession(catalogName, record.schema());
        ConnectorTableHandle handle = metadata.getTableHandle(session,
                new SchemaTableName(record.schema(), record.table()));
        if (handle == null) {
            return control.markUnavailable(record, now);
        }
        Optional<TableIdentity> identity = metadata.getTableIdentity(session, handle);
        Optional<TableVersionToken> version = metadata.observeVersion(session, handle);
        ObservationOutcome outcome = control.observeVersion(
                record, identity, version, SOURCE, now);
        if (outcome == ObservationOutcome.VERSION_CHANGED) {
            dispatchExternalCommit(record, version.orElseThrow().token(), now);
        }
        return outcome;
    }

    private void dispatchExternalCommit(TableControlRecord record, String observedToken,
            Instant observedAt) {
        if (eventDispatcher == null) {
            return;
        }
        TableRef ref = record.tableRef();
        eventDispatcher.dispatch(new MetadataEvent.ExternalCommitObservedEvent(
                new QualifiedTableRef(ref.catalog(), ref.schema(), ref.table()),
                record.tableId(), record.ownershipMode(),
                record.versionToken(), observedToken, observedAt));
    }

    private static ConnectorSession systemSession(String catalogName, String schema) {
        MetadataSessionContext context = new MetadataSessionContext(
                SYSTEM_USER, catalogName, schema, "UTC", "en",
                Map.of(), "", "", OBSERVATION_QUERY_ID);
        return DefaultConnectorSession.fromSessionContext(context, catalogName, schema);
    }

    private static final class ReportBuilder {
        private final String catalogName;
        private int checked;
        private int versionChanges;
        private int identityChanges;
        private int missing;
        private int unsupported;
        private int ignored;
        private final List<String> errors = new ArrayList<>();

        private ReportBuilder(String catalogName) {
            this.catalogName = catalogName;
        }

        private void record(ObservationOutcome outcome) {
            checked++;
            switch (outcome) {
                case VERSION_CHANGED -> versionChanges++;
                case IDENTITY_CHANGED -> identityChanges++;
                case TABLE_MISSING -> missing++;
                case UNSUPPORTED -> unsupported++;
                case IGNORED -> ignored++;
                case UNCHANGED -> { }
                default -> throw new IllegalStateException(
                        "Unexpected observation outcome: " + outcome);
            }
        }

        private void error(String message) {
            errors.add(message);
        }

        private TableVersionObservationReport build() {
            return new TableVersionObservationReport(catalogName, checked, versionChanges,
                    identityChanges, missing, unsupported, ignored, List.copyOf(errors));
        }
    }
}
