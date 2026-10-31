package org.pangrova.generation.assembly;

import org.pangrova.generation.api.publication.GenerationPublicationStore;
import org.pangrova.generation.core.publication.GenerationPublicationRouter;
import org.pangrova.common.api.annotation.Internal;

import java.util.Map;

/** Composition entry point; consumers retain the publication port, never the routing implementation. */
@Internal
public final class GenerationPublicationFactory {
    private GenerationPublicationFactory() {}

    /** Freezes the supplied domain adapters and rejects publication of unregistered artifact types. */
    public static GenerationPublicationStore route(Map<String, GenerationPublicationStore> routes) {
        return new GenerationPublicationRouter(routes);
    }
}
