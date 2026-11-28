package org.pangrova.execution.identity;

import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.generation.api.page.GenerationLease;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** One deterministic task identity for actual artifact page leases across the common dispatch path. */
@org.pangrova.common.api.annotation.Internal
public final class StageTaskIdentity {
    private StageTaskIdentity() {}

    /** Encodes the actual owner fence, immutable page identity and current page attempt. */
    public static String fromLease(GenerationLease lease) {
        byte[] execution = lease.executionAnchor().executionId().getBytes(StandardCharsets.UTF_8);
        byte[] page = lease.pageId().getBytes(StandardCharsets.UTF_8);
        int length = execution.length + page.length + 2 * Integer.BYTES + 3 * Long.BYTES;
        var identity = ByteBuffer.allocate(length)
                .putInt(execution.length).put(execution).putInt(page.length).put(page)
                .putLong(lease.executionAnchor().fencingToken())
                .putLong(lease.fencingToken()).putLong(lease.attemptId());
        return "artifact-task:" + Sha256Digests.sha256Hex(identity.array());
    }
}
