package org.pangrova.generation.api.identity;

/** Tenant-scoped durable artifact job identity. */
public record GenerationJobKey(String tenantId, String jobId) {

    public GenerationJobKey {
        requireText(tenantId, "tenantId");
        requireText(jobId, "jobId");
    }

    /** Stable routing identifier; authority is established only by the persisted exact job association. */
    public String executionId() {
        try {
            var digest = java.security.MessageDigest.getInstance("SHA-256");
            for (String value : java.util.List.of(tenantId, jobId)) {
                byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                digest.update(java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                digest.update(bytes);
            }
            return "artifact:" + java.util.HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
