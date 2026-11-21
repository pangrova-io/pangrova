package org.pangrova.common.file;

import org.pangrova.common.util.DigestUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Canonical, nonce-free identity of one admitted COPY source staging command. */
public final class CopySourceStagingDigest {
    private CopySourceStagingDigest() {}

    /** Nonce-free immutable fields in their canonical encoding order. */
    public record Scope(String tenantId, String principal, String profileName,
                        String profileIncarnationId, long profileRevision,
                        String canonicalSourceUrl, String managedStorageRootUri,
                        String managedBackendIdentitySha256,
                        String executionId, String stagingAttemptId, String ownerId,
                        String ownerIncarnation, long controlEpoch,
                        long ownerFencingToken, long deadlineEpochMs) {}

    /** Versioned, length-prefixed encoding shared by admission and task transport. */
    public static String sha256(Scope scope) {
        Objects.requireNonNull(scope, "scope");
        try (var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes)) {
            out.writeInt(2);
            writeText(out, scope.tenantId());
            writeText(out, scope.principal());
            writeText(out, scope.profileName());
            writeText(out, scope.profileIncarnationId());
            out.writeLong(scope.profileRevision());
            writeText(out, scope.canonicalSourceUrl());
            writeText(out, scope.managedStorageRootUri());
            writeText(out, scope.managedBackendIdentitySha256());
            writeText(out, scope.executionId());
            writeText(out, scope.stagingAttemptId());
            writeText(out, scope.ownerId());
            writeText(out, scope.ownerIncarnation());
            out.writeLong(scope.controlEpoch());
            out.writeLong(scope.ownerFencingToken());
            out.writeLong(scope.deadlineEpochMs());
            out.flush();
            return DigestUtils.sha256Hex(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("COPY staging command cannot be encoded", failure);
        }
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        byte[] encoded = Objects.requireNonNull(value, "COPY staging field")
                .getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }
}
