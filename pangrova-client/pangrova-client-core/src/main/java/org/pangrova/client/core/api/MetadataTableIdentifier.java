package org.pangrova.client.core.api;

import java.util.List;

/** Table identifier returned by the catalog REST API. */
public record MetadataTableIdentifier(List<String> namespace, String name) {

    public MetadataTableIdentifier {
        namespace = List.copyOf(namespace);
    }
}
