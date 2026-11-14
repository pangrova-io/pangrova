package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.SourceObjectPurpose;
import org.pangrova.common.util.DigestUtils;

import org.pangrova.common.api.annotation.Experimental;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

/** Bounded ordered SDK-owned object registration; its exact page receipt is durably idempotent. */
@Experimental
public record SourceObjectPage(int index, String afterPath, List<ObjectReference> objects,
        byte[] sha256, boolean last) {
    public static final int MAX_PAGE_OBJECTS = 512;
    public static final int MAX_PAGES = 4096;
    public static final int MAX_PAGE_BYTES = 8 * 1024 * 1024;

    private static final int MAX_OBJECT_PATH_LENGTH = 2048;


    @Experimental
    public record ObjectReference(String relativePath, long sizeBytes, SourceObjectPurpose purpose) {
        public ObjectReference {
            AcceptedSourceSnapshot.requireRelativePath(relativePath);
            if (relativePath.length() > MAX_OBJECT_PATH_LENGTH || !relativePath.matches("[A-Za-z0-9/_.=\\-]+")) {
                throw new IllegalArgumentException("SDK object path exceeds the canonical ASCII key bound");
            }
            if (sizeBytes < 0 || purpose == null) {
                throw new IllegalArgumentException("Invalid source object size or purpose");
            }
        }
    }

    public SourceObjectPage {
        objects = List.copyOf(objects);
        sha256 = sha256.clone();
        if (index < 0 || index >= MAX_PAGES || afterPath == null || objects.isEmpty()
                || objects.size() > MAX_PAGE_OBJECTS || sha256.length != 32 || (index == 0) != afterPath.isEmpty()) {
            throw new IllegalArgumentException("Invalid source object page bounds");
        }
        String previous = afterPath;
        for (var object : objects) {
            if (object.relativePath().compareTo(previous) <= 0) {
                throw new IllegalArgumentException("Object page is duplicated or unordered");
            }
            previous = object.relativePath();
        }
        if (!MessageDigest.isEqual(sha256, digest(objects))) {
            throw new IllegalArgumentException("Object page digest differs");
        }
    }
    @Override public byte[] sha256() { return sha256.clone(); }
    public String lastPath() { return objects.get(objects.size() - 1).relativePath(); }

    /**
     * Canonical object proof: unsigned 32-bit UTF-8 length, path bytes, unsigned 64-bit size,
     * unsigned 32-bit purpose (1/2), all big endian.
     */
    public static byte[] digest(List<ObjectReference> objects) {
        var digest = DigestUtils.sha256();
        int size = 0;
        for (var object : objects) {
            byte[] proof = proof(object);
            size = Math.addExact(size, proof.length);
            if (size > MAX_PAGE_BYTES) {
                throw new IllegalArgumentException("Object page exceeds its byte bound");
            }
            digest.update(proof);
        }
        return digest.digest();
    }
    /** Encodes one exact object, also used when sealing a streamed database membership. */
    public static byte[] proof(ObjectReference object) {
        try (var bytes = new ByteArrayOutputStream(); var output = new DataOutputStream(bytes)) {
            byte[] path = object.relativePath().getBytes(StandardCharsets.UTF_8);
            output.writeInt(path.length);
            output.write(path);
            output.writeLong(object.sizeBytes());
            output.writeInt(object.purpose() == SourceObjectPurpose.SOURCE ? 1 : 2);
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
