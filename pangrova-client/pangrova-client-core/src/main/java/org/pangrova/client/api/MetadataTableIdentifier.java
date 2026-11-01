package org.pangrova.client.api;

import java.util.List;

/** Table identifier returned by the catalog REST API. */
public record MetadataTableIdentifier(List<String> namespace, String name) {

    public MetadataTableIdentifier {
        namespace = List.copyOf(namespace);
    }
}
