package org.pangrova.common.client.metadata;

import java.util.List;

/** Table metadata including columns from the catalog REST API. */
public record MetadataTableInfo(List<String> namespace, String table, List<MetadataColumnInfo> columns) {

    public MetadataTableInfo {
        namespace = List.copyOf(namespace);
        columns = List.copyOf(columns);
    }
}
