package org.pangrova.persistence.file.api.copy;

import org.pangrova.common.util.DigestUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Local canonical-value checks shared by admission records; no backend or serialization framework. */
final class CopySourceAdmissionValidation {
    private static final int NONCE_BYTES = 32;
    private static final int NONCE_CHARS = 43;
    private static final int MAX_MANAGED_URI_BYTES = 2048;

    private CopySourceAdmissionValidation() {}

    static byte[] decodeNonce(String nonce) {
        if (nonce == null || nonce.length() != NONCE_CHARS) {
            throw new IllegalArgumentException("COPY source ticket is not canonical");
        }
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(nonce);
            if (bytes.length != NONCE_BYTES || !nonce.equals(
                    Base64.getUrlEncoder().withoutPadding().encodeToString(bytes))) {
                throw new IllegalArgumentException("COPY source ticket is not canonical");
            }
            return bytes;
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("COPY source ticket is not canonical", failure);
        }
    }

    static boolean isSha256(String value) {
        return DigestUtils.isSha256Hex(value);
    }

    static boolean isManagedObjectUri(String value, String scope, String checksum) {
        if (value == null || value.getBytes(StandardCharsets.UTF_8).length > MAX_MANAGED_URI_BYTES) {
            return false;
        }
        try {
            URI uri = new URI(value);
            String key = uri.getRawPath();
            String suffix = "/tenants/" + scope + "/sha256/"
                    + checksum.substring(0, 2) + "/" + checksum;
            return isCanonicalManagedS3Uri(uri)
                    && key != null && safeManagedKey(key) && key.endsWith(suffix)
                    && value.equals("s3://" + uri.getRawAuthority() + key);
        } catch (URISyntaxException | IllegalArgumentException failure) {
            return false;
        }
    }

    static boolean isManagedStorageRootUri(String value) {
        if (value == null || value.getBytes(StandardCharsets.UTF_8).length > 1024
                || value.endsWith("/")) {
            return false;
        }
        try {
            URI uri = new URI(value);
            String path = uri.getRawPath();
            return isCanonicalManagedS3Uri(uri)
                    && (path == null || path.isEmpty() || safeManagedKey(path))
                    && value.equals("s3://" + uri.getRawAuthority() + (path == null ? "" : path));
        } catch (URISyntaxException | IllegalArgumentException failure) {
            return false;
        }
    }

    static boolean isCanonicalManagedS3Uri(URI uri) {
        return "s3".equals(uri.getScheme()) && !uri.isOpaque()
                && uri.getRawAuthority() != null && !uri.getRawAuthority().isBlank()
                && uri.getHost() != null && uri.getRawAuthority().equals(uri.getHost())
                && uri.getRawUserInfo() == null && uri.getPort() == -1
                && uri.getRawQuery() == null && uri.getRawFragment() == null;
    }

    static boolean safeManagedKey(String key) {
        if (!key.startsWith("/") || key.indexOf('%') >= 0 || key.indexOf('\\') >= 0) {
            return false;
        }
        for (String segment : key.substring(1).split("/", -1)) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        return true;
    }

    static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("COPY source " + name + " is required");
        }
    }

}
