package org.pangrova.metadata.governance;

import org.pangrova.governance.runtime.classification.ColumnTagBindings;
import org.pangrova.governance.runtime.classification.ColumnTagService;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.table.api.identity.TableRef;

import java.util.Map;

/** Resolves the authenticated column and its table identity before invoking the durable tag binding. */
public final class CatalogColumnTags {
    private final CatalogRegistry catalogRegistry;
    private final ColumnTagBindings tagOperations;

    public CatalogColumnTags(CatalogRegistry catalogs) {
        catalogRegistry = catalogs;
        tagOperations = new ColumnTagBindings(catalogs.tableGovernance());
    }

    public ColumnTagService columnTagEvents() {
        return tagOperations.events();
    }

    /**
     * Re-emits every persisted tag after wiring the governance tag service, so
     * derived policy (for example auto-masks) is rebuilt after a restart.
     *
     * @return the number of tag entries replayed
     */
    public int replayColumnTags() {
        return tagOperations.replayAll();
    }

    public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                       String columnName, String tagKey, String value) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        target.requireTagType(tagKey);
        tagOperations.setTag(target.owner(), target.column(), tagKey, value, session.user());
    }

    public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                         String columnName, String tagKey) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        tagOperations.unsetTag(target.owner(), target.column(), tagKey);
    }

    public Map<String, String> listTags(MetadataSessionContext session,
                                        QualifiedTableRef tableRef, String columnName) {
        var target = MetadataColumnTagTarget.resolve(catalogRegistry, session, tableRef, columnName);
        return tagOperations.listTags(target.owner(), target.column());
    }

    public int clearTableTags(java.util.UUID tableId) {
        return tagOperations.clearTable(tableId);
    }

    public int renameTableTags(QualifiedTableRef from,
            QualifiedTableRef to) {
        return tagOperations.renameTable(new TableRef(from.catalog(), from.schema(), from.table()),
                new TableRef(to.catalog(), to.schema(), to.table()));
    }
}
