package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.indexing.control.ArtifactBuildLease;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** One deterministic task identity for actual artifact page leases across the common dispatch path. */
@org.pangrova.spi.annotation.Internal
public final class StageTaskIdentity {
    private StageTaskIdentity() {}

    /** Encodes the actual owner fence, immutable page identity and current page attempt. */
    public static String fromLease(ArtifactBuildLease lease) {
        byte[] execution = lease.executionAnchor().executionId().getBytes(StandardCharsets.UTF_8);
        byte[] page = lease.pageId().getBytes(StandardCharsets.UTF_8);
        int length = execution.length + page.length + 2 * Integer.BYTES + 3 * Long.BYTES;
        var identity = ByteBuffer.allocate(length)
                .putInt(execution.length).put(execution).putInt(page.length).put(page)
                .putLong(lease.executionAnchor().fencingToken())
                .putLong(lease.fencingToken()).putLong(lease.attemptId());
        return "artifact-task:" + DigestUtils.sha256Hex(identity.array());
    }
}
