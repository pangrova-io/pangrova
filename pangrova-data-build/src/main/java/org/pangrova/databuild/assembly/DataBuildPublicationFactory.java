package org.pangrova.databuild.assembly;

import org.pangrova.databuild.api.publication.DataBuildPublicationStore;
import org.pangrova.databuild.core.publication.DataBuildPublicationRouter;
import org.pangrova.common.api.annotation.Internal;

import java.util.Map;

/** Composition entry point; consumers retain the publication port, never the routing implementation. */
@Internal
public final class DataBuildPublicationFactory {
    private DataBuildPublicationFactory() {}

    /** Freezes the supplied domain adapters and rejects publication of unregistered artifact types. */
    public static DataBuildPublicationStore route(Map<String, DataBuildPublicationStore> routes) {
        return new DataBuildPublicationRouter(routes);
    }
}
