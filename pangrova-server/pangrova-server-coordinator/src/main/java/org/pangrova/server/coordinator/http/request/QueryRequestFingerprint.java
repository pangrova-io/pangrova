package org.pangrova.server.coordinator.http.request;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
import java.util.TreeMap;
import org.pangrova.common.client.query.SubmitQueryRequest;
import org.pangrova.common.util.DigestUtils;

/** Stateless canonicalization for durable principal-scoped query acceptance. */
public final class QueryRequestFingerprint {
    private QueryRequestFingerprint() { }

    /** Hashes the opaque client key without retaining its plaintext. */
    public static String keyDigest(String key) {
        Objects.requireNonNull(key, "key");
        return HexFormat.of().formatHex(DigestUtils.sha256().digest(key.getBytes(StandardCharsets.UTF_8)));
    }

    /** Canonicalizes SQL and sorted session properties into a fixed-size request digest. */
    public static String request(SubmitQueryRequest request) {
        Objects.requireNonNull(request, "request");
        MessageDigest digest = DigestUtils.sha256();
        update(digest, request.query());
        new TreeMap<>(request.sessionProperties()).forEach((name, value) -> {
            update(digest, name);
            update(digest, value);
        });
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
