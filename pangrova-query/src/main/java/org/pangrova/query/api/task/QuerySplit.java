package org.pangrova.query.api.task;

import java.util.List;
import org.pangrova.common.api.annotation.Internal;

/**
 * Encoded connector split with cache-affinity locations, shared by enumeration and dispatch.
 * The immutable value owns no connector session or buffer lease; encoding and identity are
 * validated at the execution boundary. Locations are routing hints, not access credentials.
 */
@Internal
public record QuerySplit(String serializedData, List<String> filePaths) {
    public QuerySplit {
        filePaths = filePaths != null ? List.copyOf(filePaths) : List.of();
    }
}
