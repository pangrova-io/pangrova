package org.pangrova.generation.api.identity;

import org.pangrova.common.crypto.Sha256Digests;

import static org.pangrova.common.validation.Preconditions.requireText;

/** Tenant-scoped durable artifact job identity. */
public record GenerationJobKey(String tenantId, String jobId) {

    public GenerationJobKey {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
    }

    /** Stable routing identifier; authority is established only by the persisted exact job association. */
    public String executionId() {
        var digest = Sha256Digests.sha256();
        for (String value : java.util.List.of(tenantId, jobId)) {
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            digest.update(java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            digest.update(bytes);
        }
        return "artifact:" + java.util.HexFormat.of().formatHex(digest.digest());
    }
}
