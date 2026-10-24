package org.pangrova.coordinator.query.statement;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.control.GovernanceBindingType;
import org.pangrova.metadata.control.OwnershipMode;
import org.pangrova.metadata.control.TableControlMaintenanceService;
import org.pangrova.metadata.control.TableControlPolicyBinder;
import org.pangrova.metadata.control.TableControlRecord;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.ColumnMaskRecord;
import org.pangrova.spi.security.ColumnMaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Table-control DDL side effects: adoption/unlinking of external tables and the
 * governance state that is keyed either by name or by stable table ID.
 *
 * <p>Kept beside {@link DdlExecutor} rather than inside it so ownership and
 * governance mutations stay readable as one unit; the executor only routes
 * statements here.</p>
 */
final class TableControlDdlOperations {

    private static final Logger LOG = LoggerFactory.getLogger(TableControlDdlOperations.class);

    private final CatalogRegistry catalogRegistry;
    private volatile TableControlMaintenanceService maintenancePolicyService;
    private volatile TableControlPolicyBinder policyBinder;
    private volatile org.pangrova.coordinator.query.audit.TableControlAuditEmitter audit;
    private volatile MetadataService metadataService;
    private volatile ColumnMaskStore columnMaskStore;

    TableControlDdlOperations(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    void setMaintenancePolicyService(TableControlMaintenanceService service) {
        this.maintenancePolicyService = service;
    }

    void setPolicyBinder(TableControlPolicyBinder binder) {
        this.policyBinder = binder;
    }

    void setAudit(org.pangrova.coordinator.query.audit.TableControlAuditEmitter emitter) {
        this.audit = emitter;
    }

    void setMetadataService(MetadataService service) {
        this.metadataService = service;
    }

    void setColumnMaskStore(ColumnMaskStore store) {
        this.columnMaskStore = store;
    }

    MetadataService metadataService() {
        return metadataService;
    }

    /** Adopts an external table as LINKED. The physical table is not modified. */
    List<TypedRow> adopt(AlterTableStatement.Adopt stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        ConnectorSession session = TableResolver.buildSession(context, ref.catalog(), ref.schema());
        ConnectorMetadata metadata = TableResolver.getMetadata(catalogRegistry, ref.catalog(), session);
        ConnectorTableHandle handle = TableResolver.requireTableHandle(metadata, session, ref);
        String owner = actor(context);
        validateAdoptionPolicy(stmt.properties());
        var identity = metadata.getTableIdentity(session, handle);
        var version = metadata.observeVersion(session, handle);
        TableControlRecord record = catalogRegistry.tableControl().adopt(
                ref.catalog(), ref.schema(), ref.table(), owner, identity, version);
        applyMaintenancePolicy(record, stmt.properties(), owner);
        var emitter = audit;
        if (emitter != null) {
            emitter.adopted(context, ref, record);
        }
        LOG.info("event=table_adopted table={} formatUuid={}", ref,
                identity.map(value -> value.formatTableUuid()).orElse("<unknown>"));
        return List.of();
    }

    /** Removes LINKED control state. The physical table and its data are untouched. */
    List<TypedRow> unlink(AlterTableStatement.Unlink stmt, CoordinatorSessionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(stmt.tableName().parts(), context);
        TableControlRecord record = catalogRegistry.tableControl().unlink(
                ref.catalog(), ref.schema(), ref.table());
        purgeTableState(record, context);
        var emitter = audit;
        if (emitter != null) {
            emitter.unlinked(context, ref, record);
        }
        LOG.info("event=table_unlinked table={}", ref);
        return List.of();
    }

    /**
     * Drops state scoped to a retired control record. Governance bindings and
     * collected statistics are keyed by the stable table_id, so leaving them
     * behind would attach the old table's policy and cardinalities to any record
     * that reused the ID.
     */
    void purgeTableState(TableControlRecord record, CoordinatorSessionContext context) {
        if (record == null) {
            return;
        }
        var binder = policyBinder;
        if (binder != null) {
            binder.purge(record.tableId());
        }
        MetadataService service = metadataService;
        if (service != null) {
            service.clearTableTags(SessionContextConverter.toMetadataSessionContext(context),
                    new QualifiedTableRef(record.catalog(), record.schema(), record.table()));
        }
        catalogRegistry.tableStatistics().purge(record.tableId());
    }

    void requireBindable(QualifiedTableName ref) {
        if (!catalogRegistry.tableControl().enabled()) {
            return;
        }
        requireBinder().requireBindable(ref.catalog(), ref.schema(), ref.table());
    }

    void bindColumnTag(QualifiedTableName ref, String columnName, String tagKey,
            String tagValue, CoordinatorSessionContext context) {
        if (!catalogRegistry.tableControl().enabled()) {
            return;
        }
        requireBinder().bind(ref.catalog(), ref.schema(), ref.table(),
                GovernanceBindingType.COLUMN_TAG, tagBindingRef(columnName, tagKey),
                Map.of("value", tagValue), actor(context));
    }

    void unbindColumnTag(QualifiedTableName ref, String columnName, String tagKey) {
        if (!catalogRegistry.tableControl().enabled()) {
            return;
        }
        requireBinder().unbind(ref.catalog(), ref.schema(), ref.table(),
                GovernanceBindingType.COLUMN_TAG, tagBindingRef(columnName, tagKey));
    }

    /**
     * Carries name-keyed governance across a rename. Durable tags are moved
     * first, which re-derives tag-based masks at the new name and withdraws them
     * from the old one; remaining masks (manually authored, not tag-derived) are
     * re-keyed explicitly so a renamed table is never left unmasked.
     */
    void moveNameScopedGovernance(QualifiedTableName from, QualifiedTableName to,
            List<String> columns, CoordinatorSessionContext context) {
        MetadataService service = metadataService;
        if (service != null) {
            service.renameTableTags(SessionContextConverter.toMetadataSessionContext(context),
                    new QualifiedTableRef(from.catalog(), from.schema(), from.table()),
                    new QualifiedTableRef(to.catalog(), to.schema(), to.table()));
        }
        var store = columnMaskStore;
        if (store == null) {
            return;
        }
        int moved = 0;
        for (String column : columns) {
            for (var mask : store.getMasks(from.catalog(), from.schema(), from.table(), column)) {
                store.saveMask(new ColumnMaskRecord(0, mask.maskName(),
                        to.catalog(), to.schema(), to.table(), column, mask.maskExpression(),
                        mask.applyToRoles(), mask.exemptRoles(), mask.enabled()));
                store.deleteMask(mask.maskId());
                moved++;
            }
        }
        if (moved > 0) {
            LOG.info("event=column_masks_migrated from={} to={} masks={}", from, to, moved);
        }
    }

    /** Drops masks keyed by the retired name so a recreated table starts ungoverned. */
    void purgeColumnMasks(QualifiedTableName ref, List<String> columns) {
        var store = columnMaskStore;
        if (store == null || columns.isEmpty()) {
            return;
        }
        int removed = 0;
        for (String column : columns) {
            for (var mask : store.getMasks(ref.catalog(), ref.schema(), ref.table(), column)) {
                store.deleteMask(mask.maskId());
                removed++;
            }
        }
        if (removed > 0) {
            LOG.info("event=column_masks_purged table={} masks={}", ref, removed);
        }
    }

    private TableControlPolicyBinder requireBinder() {
        var binder = policyBinder;
        if (binder == null) {
            throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                    "ALTER TABLE column tags require governance binding storage");
        }
        return binder;
    }

    private void validateAdoptionPolicy(Map<String, String> properties) {
        var service = maintenancePolicyService;
        if (service == null) {
            if (!properties.isEmpty()) {
                throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                        "ADOPT WITH is unavailable: maintenance policy storage is not configured");
            }
            return;
        }
        service.validateAdoptionProperties(OwnershipMode.LINKED, properties);
    }

    private void applyMaintenancePolicy(TableControlRecord record,
            Map<String, String> properties, String actor) {
        var service = maintenancePolicyService;
        if (service == null) {
            if (!properties.isEmpty()) {
                throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                        "ADOPT WITH is unavailable: maintenance policy storage is not configured");
            }
            return;
        }
        service.applyAdoptionProperties(record, properties, actor);
    }

    private static String tagBindingRef(String columnName, String tagKey) {
        return columnName + ":" + tagKey;
    }

    private static String actor(CoordinatorSessionContext context) {
        return context.userIdentity() == null ? "" : context.userIdentity().principal();
    }
}
