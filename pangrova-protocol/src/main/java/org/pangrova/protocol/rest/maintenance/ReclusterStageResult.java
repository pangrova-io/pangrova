package org.pangrova.protocol.rest.maintenance;

import java.util.List;
import java.util.UUID;

/** Connector file descriptors produced by one staging attempt. */
public record ReclusterStageResult(UUID attemptId, List<String> files) {
    public ReclusterStageResult {
        java.util.Objects.requireNonNull(attemptId, "attemptId");
        files = List.copyOf(files);
    }
}
