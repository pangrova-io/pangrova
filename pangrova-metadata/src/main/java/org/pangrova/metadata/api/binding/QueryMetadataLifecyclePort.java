package org.pangrova.metadata.api.binding;

import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.spi.connector.ConnectorQueryAuthority;

/** Query-scoped metadata lifecycle use cases. */
public interface QueryMetadataLifecyclePort {

    void beginQuery(MetadataSessionContext session);

    void cleanupQuery(ConnectorQueryAuthority authority);
}
