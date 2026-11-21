package org.pangrova.metadata.api.catalog;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.table.api.ownership.TableReadAuthority;

/** Borrowed catalog handles and ownership checks for authenticated table resolution. */
@Internal
public interface CatalogReadAccess {
    /** Returns the current handle or throws when the catalog is absent or unavailable. */
    CatalogHandle getCatalog(String name);

    /** Returns metadata retained by the session's query lifetime, when it carries one. */
    ConnectorMetadata metadataForQuery(String catalog, ConnectorSession session);

    TableReadAuthority tableControl();
}
